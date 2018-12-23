/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.databasemetadata;

import static org.junit.Assert.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
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
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDatabaseMetaData;
import com.microsoft.sqlserver.jdbc.StringUtils;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;


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
    @Tag("AzureDWTest")
    public void testDatabaseMetaDataWrapper() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString)) {
            DatabaseMetaData databaseMetaData = con.getMetaData();
            assertTrue(databaseMetaData.isWrapperFor(DatabaseMetaData.class));
            assertSame(databaseMetaData, databaseMetaData.unwrap(DatabaseMetaData.class));
        }
    }

    /**
     * Testing if driver version is matching with manifest file or not. Will be useful while releasing preview / RTW
     * release.
     * 
     * //TODO: OSGI: Test for capability 1.7 for JDK 1.7 and 1.8 for 1.8 //Require-Capability:
     * osgi.ee;filter:="(&(osgi.ee=JavaSE)(version=1.8))" //String capability =
     * attributes.getValue("Require-Capability");
     * 
     * @throws SQLException
     *         SQL Exception
     * @throws IOException
     *         IOExcption
     */
    @Test
    @Tag("AzureDWTest")
    public void testDriverVersion() throws SQLException, IOException {
        String manifestFile = TestUtils.getCurrentClassPath() + "META-INF/MANIFEST.MF";
        manifestFile = manifestFile.replace("test-classes", "classes");

        File f = new File(manifestFile);

        assumeTrue(f.exists(), TestResource.getResource("R_manifestNotFound"));

        try (InputStream in = new BufferedInputStream(new FileInputStream(f))) {
            Manifest manifest = new Manifest(in);
            Attributes attributes = manifest.getMainAttributes();
            String buildVersion = attributes.getValue("Bundle-Version");

            try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString)) {

                DatabaseMetaData dbmData = conn.getMetaData();

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
                assertTrue(intDriverVersion == intBuildVersion, TestResource.getResource("R_buildVersionError"));
            }
        }
    }

    /**
     * Your password should not be in getURL method.
     * 
     * @throws SQLException
     */
    @Test
    @Tag("AzureDWTest")
    public void testGetURL() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString)) {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String url = databaseMetaData.getURL();
            url = url.toLowerCase();
            assertFalse(url.contains("password"), TestResource.getResource("R_getURLContainsPwd"));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        }
    }

    /**
     * Test getUsername.
     * 
     * @throws SQLException
     */
    @Test
    public void testDBUserLogin() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString)) {
            DatabaseMetaData databaseMetaData = conn.getMetaData();

            String connectionString = getConnectionString();

            connectionString = connectionString.toLowerCase();

            int startIndex = 0;
            int endIndex = 0;

            if (connectionString.contains("username")) {
                startIndex = connectionString.indexOf("username=");
                endIndex = connectionString.indexOf(";", startIndex);
                startIndex = startIndex + "username=".length();
            } else if (connectionString.contains("user")) {
                startIndex = connectionString.indexOf("user=");
                endIndex = connectionString.indexOf(";", startIndex);
                startIndex = startIndex + "user=".length();
            }

            String userFromConnectionString = connectionString.substring(startIndex, endIndex);
            String userName = databaseMetaData.getUserName();

            assertNotNull(userName, TestResource.getResource("R_userNameNull"));

            assertTrue(userName.equalsIgnoreCase(userFromConnectionString),
                    TestResource.getResource("R_userNameNotMatch"));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        }
    }

    /**
     * Testing of {@link SQLServerDatabaseMetaData#getSchemas()}
     * 
     * @throws SQLException
     */
    @Test
    @Tag("AzureDWTest")
    public void testDBSchema() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
                ResultSet rs = conn.getMetaData().getSchemas()) {

            MessageFormat form = new MessageFormat(TestResource.getResource("R_nameEmpty"));
            Object[] msgArgs = {"Schema"};
            while (rs.next()) {
                assertTrue(!StringUtils.isEmpty(rs.getString(1)), form.format(msgArgs));
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        }
    }

    /**
     * Tests that the catalog parameter containing - is escaped by
     * {@link SQLServerDatabaseMetaData#getSchemas(String catalog, String schemaPattern)}.
     * 
     * @throws SQLException
     */
    @Test
    public void testDBSchemasForDashedCatalogName() throws SQLException {
        UUID id = UUID.randomUUID();
        String testCatalog = "dash-catalog" + id;
        String testSchema = "some-schema" + id;

        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = connection.createStatement()) {
            TestUtils.dropDatabaseIfExists(testCatalog, stmt);
            stmt.execute(String.format("CREATE DATABASE [%s]", testCatalog));

            stmt.execute(String.format("USE [%s]", testCatalog));
            stmt.execute(String.format("CREATE SCHEMA [%s]", testSchema));

            try (ResultSet rs = conn.getMetaData().getSchemas(testCatalog, null)) {

                MessageFormat schemaEmptyFormat = new MessageFormat(TestResource.getResource("R_nameEmpty"));
                Object[] schemaMsgArgs = {"Schema"};

                boolean hasResults = false;
                boolean hasDashCatalogSchema = false;
                while (rs.next()) {
                    hasResults = true;
                    String schemaName = rs.getString(1);
                    assertTrue(!StringUtils.isEmpty(schemaName), schemaEmptyFormat.format(schemaMsgArgs));
                    String catalogName = rs.getString(2);
                    if (schemaName.equals(testSchema)) {
                        hasDashCatalogSchema = true;
                        assertEquals(catalogName, testCatalog);
                    } else {
                        assertNull(catalogName);
                    }
                }

                MessageFormat atLeastOneFoundFormat = new MessageFormat(TestResource.getResource("R_atLeastOneFound"));
                assertTrue(hasResults, atLeastOneFoundFormat.format(schemaMsgArgs));

                MessageFormat dashCatalogFormat = new MessageFormat(TestResource.getResource("R_atLeastOneFound"));
                assertTrue(hasDashCatalogSchema, dashCatalogFormat.format(new Object[] {testSchema}));
            } finally {
                TestUtils.dropDatabaseIfExists(testCatalog, stmt);
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());

        }
    }

    /**
     * Tests that the catalog parameter containing - is escaped by
     * {@link SQLServerDatabaseMetaData#getSchemas(String catalog, String schemaPattern)}.
     * 
     * @throws SQLException
     */
    @Test
    public void testDBSchemasForDashedCatalogNameWithPattern() throws SQLException {
        UUID id = UUID.randomUUID();
        String testCatalog = "dash-catalog" + id;
        String testSchema = "some-schema" + id;

        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            TestUtils.dropDatabaseIfExists(testCatalog, stmt);
            stmt.execute(String.format("CREATE DATABASE [%s]", testCatalog));

            stmt.execute(String.format("USE [%s]", testCatalog));
            stmt.execute(String.format("CREATE SCHEMA [%s]", testSchema));

            try (ResultSet rs = conn.getMetaData().getSchemas(testCatalog, "some-%")) {

                MessageFormat schemaEmptyFormat = new MessageFormat(TestResource.getResource("R_nameEmpty"));
                Object[] schemaMsgArgs = {testSchema};
                Object[] catalogMsgArgs = {testCatalog};

                boolean hasResults = false;
                while (rs.next()) {
                    hasResults = true;
                    String schemaName = rs.getString(1);
                    String catalogName = rs.getString(2);
                    assertTrue(!StringUtils.isEmpty(schemaName), schemaEmptyFormat.format(schemaMsgArgs));
                    assertTrue(!StringUtils.isEmpty(catalogName), schemaEmptyFormat.format(catalogMsgArgs));
                    assertEquals(schemaName, schemaMsgArgs[0]);
                    assertEquals(catalogName, catalogMsgArgs[0]);
                }

                MessageFormat atLeastOneFoundFormat = new MessageFormat(TestResource.getResource("R_atLeastOneFound"));
                assertTrue(hasResults, atLeastOneFoundFormat.format(schemaMsgArgs));
            } finally {
                TestUtils.dropDatabaseIfExists(testCatalog, stmt);
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());

        }
    }

    /**
     * Get All Tables.
     * 
     * @throws SQLException
     */
    @Test
    @Tag("AzureDWTest")
    /*
     * try (ResultSet rsCatalog = connection.getMetaData().getCatalogs(); ResultSet rs = connection.getMetaData()
     * .getTables(rsCatalog.getString("TABLE_CAT"), null, "%", new String[] {"TABLE"})) {
     */
    public void testDBTables() throws SQLException {

        try (Connection con = DriverManager.getConnection(connectionString)) {
            DatabaseMetaData databaseMetaData = con.getMetaData();
            try (ResultSet rsCatalog = databaseMetaData.getCatalogs()) {

                MessageFormat form1 = new MessageFormat(TestResource.getResource("R_atLeastOneFound"));
                Object[] msgArgs1 = {"catalog"};
                assertTrue(rsCatalog.next(), form1.format(msgArgs1));

                String[] types = {"TABLE"};
                try (ResultSet rs = databaseMetaData.getTables(rsCatalog.getString("TABLE_CAT"), null, "%", types)) {

                    MessageFormat form2 = new MessageFormat(TestResource.getResource("R_nameEmpty"));
                    Object[] msgArgs2 = {"Table"};
                    while (rs.next()) {
                        assertTrue(!StringUtils.isEmpty(rs.getString("TABLE_NAME")), form2.format(msgArgs2));
                    }
                }
            }
        }
    }

    /**
     * Testing DB Columns.
     * <p>
     * We can Improve this test scenario by following way.
     * <ul>
     * <li>Create table with appropriate column size, data types,auto increment, NULLABLE etc.
     * <li>Then get databasemetatadata.getColumns to see if there is any mismatch.
     * </ul>
     * 
     * @throws SQLException
     */
    @Test
    @Tag("AzureDWTest")
    public void testGetDBColumn() throws SQLException {

        try (Connection conn = DriverManager.getConnection(connectionString)) {
            DatabaseMetaData databaseMetaData = conn.getMetaData();

            String[] types = {"TABLE"};
            try (ResultSet rs = databaseMetaData.getTables(null, null, "%", types)) {

                // Fetch one table
                MessageFormat form1 = new MessageFormat(TestResource.getResource("R_atLeastOneFound"));
                Object[] msgArgs1 = {"table"};
                assertTrue(rs.next(), form1.format(msgArgs1));

                // Go through all columns.
                try (ResultSet rs1 = databaseMetaData.getColumns(null, null, rs.getString("TABLE_NAME"), "%")) {

                    MessageFormat form2 = new MessageFormat(TestResource.getResource("R_nameEmpty"));
                    Object[][] msgArgs2 = {{"Category"}, {"SCHEMA"}, {"Table"}, {"COLUMN"}, {"Data Type"}, {"Type"},
                            {"Column Size"}, {"Nullable value"}, {"IS_NULLABLE"}, {"IS_AUTOINCREMENT"}};
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
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        }
    }

    /**
     * We can improve this test case by following manner:
     * <ul>
     * <li>We can check if PRIVILEGE is in between CRUD / REFERENCES / SELECT / INSERT etc.
     * <li>IS_GRANTABLE can have only 2 values YES / NO
     * </ul>
     * 
     * @throws SQLException
     */
    @Test
    public void testGetColumnPrivileges() throws SQLException {

        try (Connection conn = DriverManager.getConnection(connectionString)) {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String[] types = {"TABLE"};
            try (ResultSet rsTables = databaseMetaData.getTables(null, null, "%", types)) {

                // Fetch one table
                MessageFormat form1 = new MessageFormat(TestResource.getResource("R_atLeastOneFound"));
                Object[] msgArgs1 = {"table"};
                assertTrue(rsTables.next(), form1.format(msgArgs1));

                try (ResultSet rs1 = databaseMetaData.getColumnPrivileges(null, null, rsTables.getString("TABLE_NAME"),
                        "%")) {

                    MessageFormat form2 = new MessageFormat(TestResource.getResource("R_nameEmpty"));
                    Object[][] msgArgs2 = {{"Category"}, {"SCHEMA"}, {"Table"}, {"COLUMN"}, {"GRANTOR"}, {"GRANTEE"},
                            {"PRIVILEGE"}, {"IS_GRANTABLE"}};
                    while (rs1.next()) {
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
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        }
    }

    /**
     * Testing {@link SQLServerDatabaseMetaData#getFunctions(String, String, String)} with sending wrong catalog.
     * 
     * @throws SQLException
     */
    @Test
    @Tag("AzureDWTest")
    public void testGetFunctionsWithWrongParams() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            conn.getMetaData().getFunctions("", null, "xp_%");
            assertTrue(false, TestResource.getResource("R_noSchemaShouldFail"));
        } catch (Exception ae) {}
    }

    /**
     * Test {@link SQLServerDatabaseMetaData#getFunctions(String, String, String)}
     * 
     * @throws SQLException
     */
    @Test
    @Tag("AzureDWTest")
    public void testGetFunctions() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString);
                ResultSet rs = conn.getMetaData().getFunctions(null, null, "xp_%")) {

            MessageFormat form = new MessageFormat(TestResource.getResource("R_nameNull"));
            Object[][] msgArgs = {{"FUNCTION_CAT"}, {"FUNCTION_SCHEM"}, {"FUNCTION_NAME"}, {"NUM_INPUT_PARAMS"},
                    {"NUM_OUTPUT_PARAMS"}, {"NUM_RESULT_SETS"}, {"FUNCTION_TYPE"}};
            while (rs.next()) {
                assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_CAT")), form.format(msgArgs[0]));
                assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_SCHEM")), form.format(msgArgs[1]));
                assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_NAME")), form.format(msgArgs[2]));
                assertTrue(!StringUtils.isEmpty(rs.getString("NUM_INPUT_PARAMS")), form.format(msgArgs[3]));
                assertTrue(!StringUtils.isEmpty(rs.getString("NUM_OUTPUT_PARAMS")), form.format(msgArgs[4]));
                assertTrue(!StringUtils.isEmpty(rs.getString("NUM_RESULT_SETS")), form.format(msgArgs[5]));
                assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_TYPE")), form.format(msgArgs[6]));
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        }
    }

    /**
     * 
     * @throws SQLException
     */
    @Test
    @Tag("AzureDWTest")
    public void testGetFunctionColumns() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString)) {

            DatabaseMetaData databaseMetaData = conn.getMetaData();

            try (ResultSet rsFunctions = databaseMetaData.getFunctions(null, null, "%")) {

                // Fetch one Function
                MessageFormat form1 = new MessageFormat(TestResource.getResource("R_atLeastOneFound"));
                Object[] msgArgs1 = {"function"};
                assertTrue(rsFunctions.next(), form1.format(msgArgs1));

                try (ResultSet rs = databaseMetaData.getFunctionColumns(null, null,
                        rsFunctions.getString("FUNCTION_NAME"), "%")) {

                    MessageFormat form2 = new MessageFormat(TestResource.getResource("R_nameNull"));
                    Object[][] msgArgs2 = {{"FUNCTION_CAT"}, {"FUNCTION_SCHEM"}, {"FUNCTION_NAME"}, {"COLUMN_NAME"},
                            {"COLUMN_TYPE"}, {"DATA_TYPE"}, {"TYPE_NAME"}, {"NULLABLE"}, {"IS_NULLABLE"}};
                    while (rs.next()) {
                        assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_CAT")), form2.format(msgArgs2[0]));
                        assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_SCHEM")), form2.format(msgArgs2[1]));
                        assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_NAME")), form2.format(msgArgs2[2]));
                        assertTrue(!StringUtils.isEmpty(rs.getString("COLUMN_NAME")), form2.format(msgArgs2[3]));
                        assertTrue(!StringUtils.isEmpty(rs.getString("COLUMN_TYPE")), form2.format(msgArgs2[4]));
                        assertTrue(!StringUtils.isEmpty(rs.getString("DATA_TYPE")), form2.format(msgArgs2[5]));
                        assertTrue(!StringUtils.isEmpty(rs.getString("TYPE_NAME")), form2.format(msgArgs2[6]));
                        assertTrue(!StringUtils.isEmpty(rs.getString("NULLABLE")), form2.format(msgArgs2[7])); // 12
                        assertTrue(!StringUtils.isEmpty(rs.getString("IS_NULLABLE")), form2.format(msgArgs2[8])); // 19
                    }
                }
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        }
    }

    @Test
    public void testPreparedStatementMetadataCaching() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString)) {

            DatabaseMetaData databaseMetaData = connection.getMetaData();
            String[] types = {"TABLE"};

            Statement stmtNullCatalog;
            Statement stmtMasterCatalog;

            try (ResultSet rs = databaseMetaData.getTables(null, null, "%", types)) {
                stmtNullCatalog = rs.getStatement();
            }
            try (ResultSet rs = databaseMetaData.getTables(null, null, "%", types)) {
                assertSame(stmtNullCatalog, rs.getStatement());
                rs.getStatement().close();
            }
            try (ResultSet rs = databaseMetaData.getTables(null, null, "%", types)) {
                assertNotSame(stmtNullCatalog, rs.getStatement());
                stmtNullCatalog = rs.getStatement();
            }
            try (ResultSet rs = databaseMetaData.getTables(null, null, "%", types)) {
                assertSame(stmtNullCatalog, rs.getStatement());
            }
            try (ResultSet rs = databaseMetaData.getTables("master", null, "%", types)) {
                stmtMasterCatalog = rs.getStatement();
            }
            try (ResultSet rs = databaseMetaData.getTables("master", null, "%", types)) {
                assertSame(stmtMasterCatalog, rs.getStatement());
                rs.getStatement().close();
            }
            try (ResultSet rs = databaseMetaData.getTables("master", null, "%", types)) {
                assertNotSame(stmtMasterCatalog, rs.getStatement());
                stmtMasterCatalog = rs.getStatement();
            }
            try (ResultSet rs = databaseMetaData.getTables("master", null, "%", types)) {
                assertSame(stmtMasterCatalog, rs.getStatement());
            }
            try (ResultSet rs = databaseMetaData.getTables(null, null, "%", types)) {
                assertSame(stmtNullCatalog, rs.getStatement());
                rs.getStatement().close();
            }
            try (ResultSet rs = databaseMetaData.getTables("master", null, "%", types)) {
                assertSame(stmtMasterCatalog, rs.getStatement());
                rs.getStatement().close();
            }
        }
    }
}
