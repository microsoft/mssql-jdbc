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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDatabaseMetaData;
import com.microsoft.sqlserver.jdbc.StringUtils;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Test class for testing DatabaseMetaData.
 */
@RunWith(JUnitPlatform.class)
public class DatabaseMetaDataTest extends AbstractTest {

    private static final String tableName = RandomUtil.getIdentifier("DBMetadataTable");
    private static final String functionName = RandomUtil.getIdentifier("DBMetadataFunction");
    private static LinkedHashMap<Integer, String> getColumnsDWColumns = null;
    private static LinkedHashMap<Integer, String> getImportedKeysDWColumns = null;

    /**
     * Verify DatabaseMetaData#isWrapperFor and DatabaseMetaData#unwrap.
     * 
     * @throws SQLException
     */
    @Test
    public void testDatabaseMetaDataWrapper() throws SQLException {
        try (Connection con = getConnection()) {
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
    @Tag(Constants.xGradle)
    public void testDriverVersion() throws SQLException, IOException {
        String manifestFile = TestUtils.getCurrentClassPath() + "META-INF/MANIFEST.MF";
        manifestFile = manifestFile.replace("test-classes", "classes");

        File f = new File(manifestFile);

        try (InputStream in = new BufferedInputStream(new FileInputStream(f))) {
            Manifest manifest = new Manifest(in);
            Attributes attributes = manifest.getMainAttributes();
            String buildVersion = attributes.getValue("Bundle-Version");

            try (Connection conn = getConnection()) {
                DatabaseMetaData dbmData = conn.getMetaData();
                String driverVersion = dbmData.getDriverVersion();

                // boolean isSnapshot = buildVersion.contains("SNAPSHOT");

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
    public void testGetURL() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String url = databaseMetaData.getURL();
            url = url.toLowerCase();
            assertFalse(url.contains("password"), TestResource.getResource("R_getURLContainsPwd"));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    /**
     * Test getUsername.
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    public void testDBUserLogin() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String connectionString = getConnectionString();
            connectionString = connectionString.toLowerCase();

            int startIndex = 0;
            int endIndex = 0;

            if (connectionString.contains("username")) {
                startIndex = connectionString.indexOf("username=");
                endIndex = connectionString.indexOf(Constants.SEMI_COLON, startIndex);
                startIndex = startIndex + "username=".length();
            } else if (connectionString.contains("user")) {
                startIndex = connectionString.indexOf("user=");
                endIndex = connectionString.indexOf(Constants.SEMI_COLON, startIndex);
                startIndex = startIndex + "user=".length();
            }

            String userFromConnectionString = connectionString.substring(startIndex, endIndex);
            String userName = databaseMetaData.getUserName();

            assertNotNull(userName, TestResource.getResource("R_userNameNull"));
            assertTrue(userName.equalsIgnoreCase(userFromConnectionString),
                    TestResource.getResource("R_userNameNotMatch"));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    /**
     * Testing of {@link SQLServerDatabaseMetaData#getSchemas()}
     * 
     * @throws SQLException
     */
    @Test
    public void testDBSchema() throws SQLException {
        try (Connection conn = getConnection(); ResultSet rs = conn.getMetaData().getSchemas()) {

            MessageFormat form = new MessageFormat(TestResource.getResource("R_nameEmpty"));
            Object[] msgArgs = {"Schema"};
            while (rs.next()) {
                assertTrue(!StringUtils.isEmpty(rs.getString(1)), form.format(msgArgs));
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    /**
     * Tests that the catalog parameter containing - is escaped by
     * {@link SQLServerDatabaseMetaData#getSchemas(String catalog, String schemaPattern)}.
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    public void testDBSchemasForDashedCatalogName() throws SQLException {
        UUID id = UUID.randomUUID();
        String testCatalog = "dash-catalog" + id;
        String testSchema = "some-schema" + id;

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropDatabaseIfExists(testCatalog, connectionString);
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
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        } finally {
            TestUtils.dropDatabaseIfExists(testCatalog, connectionString);
        }
    }

    /**
     * Tests that the catalog parameter containing - is escaped by
     * {@link SQLServerDatabaseMetaData#getSchemas(String catalog, String schemaPattern)}.
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    public void testDBSchemasForDashedCatalogNameWithPattern() throws SQLException {
        UUID id = UUID.randomUUID();
        String testCatalog = "dash-catalog" + id;
        String testSchema = "some-schema" + id;

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropDatabaseIfExists(testCatalog, connectionString);
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
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        } finally {
            TestUtils.dropDatabaseIfExists(testCatalog, connectionString);
        }
    }

    /**
     * Get All Tables.
     * 
     * @throws SQLException
     */
    @Test
    public void testDBTables() throws SQLException {

        try (Connection con = getConnection()) {
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
    public void testGetDBColumn() throws SQLException {

        try (Connection conn = getConnection()) {
            DatabaseMetaData databaseMetaData = conn.getMetaData();

            String[] types = {"TABLE"};
            boolean tableFound = false;

            try (ResultSet rs = databaseMetaData.getTables(null, null, "%", types)) {
                if (rs.next()) {
                    do {
                        if (rs.getString("TABLE_NAME").equalsIgnoreCase(tableName)) {
                            tableFound = true;
                        }
                    } while (rs.next() && !tableFound);
                }

                if (!tableFound) {
                    MessageFormat form1 = new MessageFormat(TestResource.getResource("R_tableNotFound"));
                    Object[] msgArgs1 = {tableName};
                    fail(form1.format(msgArgs1));
                } else {
                    try (ResultSet rs1 = databaseMetaData.getColumns(null, null, tableName, "%")) {
                        testGetDBColumnInternal(rs1, databaseMetaData);
                    }

                    try (ResultSet rs1 = databaseMetaData.getColumns(null, null, tableName, "col\\_1")) {
                        testGetDBColumnInternal(rs1, databaseMetaData);
                    }

                    try (ResultSet rs1 = databaseMetaData.getColumns(null, null, tableName, "col\\%2")) {
                        testGetDBColumnInternal(rs1, databaseMetaData);
                    }

                    try (ResultSet rs1 = databaseMetaData.getColumns(null, null, tableName, "col\\[3")) {
                        testGetDBColumnInternal(rs1, databaseMetaData);
                    }
                }
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    private void testGetDBColumnInternal(ResultSet rs1, DatabaseMetaData databaseMetaData) throws SQLException {
        MessageFormat form2 = new MessageFormat(TestResource.getResource("R_nameEmpty"));
        Object[][] msgArgs2 = {{"Category"}, {"SCHEMA"}, {"Table"}, {"COLUMN"}, {"Data Type"}, {"Type"},
                {"Column Size"}, {"Nullable value"}, {"IS_NULLABLE"}, {"IS_AUTOINCREMENT"}};
        if (!rs1.next()) {
            fail(TestResource.getResource("R_resultSetEmpty"));
        } else {
            do {
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
            } while (rs1.next());
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
    @Tag(Constants.xAzureSQLDW)
    public void testGetColumnPrivileges() throws SQLException {

        try (Connection conn = getConnection()) {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String[] types = {"TABLE"};
            boolean tableFound = false;

            try (ResultSet rs = databaseMetaData.getTables(null, null, "%", types)) {
                if (rs.next()) {
                    do {
                        if (rs.getString("TABLE_NAME").equalsIgnoreCase(tableName)) {
                            tableFound = true;
                        }
                    } while (rs.next() && !tableFound);
                }

                if (!tableFound) {
                    MessageFormat form1 = new MessageFormat(TestResource.getResource("R_tableNotFound"));
                    Object[] msgArgs1 = {tableName};
                    fail(form1.format(msgArgs1));
                } else {
                    try (ResultSet rs1 = databaseMetaData.getColumnPrivileges(null, null,
                            AbstractSQLGenerator.escapeIdentifier(tableName), "%")) {

                        MessageFormat form2 = new MessageFormat(TestResource.getResource("R_nameEmpty"));
                        Object[][] msgArgs2 = {{"Category"}, {"SCHEMA"}, {"Table"}, {"COLUMN"}, {"GRANTOR"},
                                {"GRANTEE"}, {"PRIVILEGE"}, {"IS_GRANTABLE"}};
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
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    /**
     * Testing {@link SQLServerDatabaseMetaData#getFunctions(String, String, String)} with sending wrong catalog.
     * 
     * @throws SQLException
     */
    @Test
    public void testGetFunctionsWithWrongParams() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.getMetaData().getFunctions("", null, "xp_%");
            fail(TestResource.getResource("R_noSchemaShouldFail"));
        } catch (SQLException e) {
            assert (e.getMessage().matches(TestUtils.formatErrorMsg("R_invalidArgument")));
        }
    }

    /**
     * Test {@link SQLServerDatabaseMetaData#getFunctions(String, String, String)}
     * 
     * @throws SQLException
     */
    @Test
    public void testGetFunctions() throws SQLException {
        try (Connection conn = getConnection(); ResultSet rs = conn.getMetaData().getFunctions(null, null, "xp_%")) {

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
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    /**
     * 
     * @throws SQLException
     */
    @Test
    public void testGetFunctionColumns() throws SQLException {
        try (Connection conn = getConnection()) {

            DatabaseMetaData databaseMetaData = conn.getMetaData();
            boolean functionFound = false;

            try (ResultSet rsFunctions = databaseMetaData.getFunctions(null, null, "%")) {
                if (rsFunctions.next()) {
                    do {
                        if (rsFunctions.getString("FUNCTION_NAME").equalsIgnoreCase(functionName)) {
                            functionFound = true;
                        }
                    } while (rsFunctions.next() && !functionFound);
                }

                if (!functionFound) {
                    try (ResultSet rs = databaseMetaData.getFunctionColumns(null, null,
                            AbstractSQLGenerator.escapeIdentifier(functionName), "%")) {

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
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    public void testPreparedStatementMetadataCaching() throws SQLException {
        try (Connection connection = getConnection()) {

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

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testGetMaxConnections() throws SQLException {
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt
                .executeQuery("select maximum from sys.configurations where name = 'user connections'")) {
            assert (null != rs);
            rs.next();

            DatabaseMetaData databaseMetaData = connection.getMetaData();
            int maxConn = databaseMetaData.getMaxConnections();

            assertEquals(maxConn, rs.getInt(1));
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testGetColumn() throws SQLException {
        try (Connection conn = getConnection();) {
            DatabaseMetaData databaseMetaData = conn.getMetaData();

            getColumnsDWColumns = new LinkedHashMap<>();
            getColumnsDWColumns.put(1, "TABLE_CAT");
            getColumnsDWColumns.put(2, "TABLE_SCHEM");
            getColumnsDWColumns.put(3, "TABLE_NAME");
            getColumnsDWColumns.put(4, "COLUMN_NAME");
            getColumnsDWColumns.put(5, "DATA_TYPE");
            getColumnsDWColumns.put(6, "TYPE_NAME");
            getColumnsDWColumns.put(7, "COLUMN_SIZE");
            getColumnsDWColumns.put(8, "BUFFER_LENGTH");
            getColumnsDWColumns.put(9, "DECIMAL_DIGITS");
            getColumnsDWColumns.put(10, "NUM_PREC_RADIX");
            getColumnsDWColumns.put(11, "NULLABLE");
            getColumnsDWColumns.put(12, "REMARKS");
            getColumnsDWColumns.put(13, "COLUMN_DEF");
            getColumnsDWColumns.put(14, "SQL_DATA_TYPE");
            getColumnsDWColumns.put(15, "SQL_DATETIME_SUB");
            getColumnsDWColumns.put(16, "CHAR_OCTET_LENGTH");
            getColumnsDWColumns.put(17, "ORDINAL_POSITION");
            getColumnsDWColumns.put(18, "IS_NULLABLE");
            getColumnsDWColumns.put(-1, "SCOPE_CATALOG");
            getColumnsDWColumns.put(-2, "SCOPE_SCHEMA");
            getColumnsDWColumns.put(-3, "SCOPE_TABLE");
            getColumnsDWColumns.put(29, "SOURCE_DATA_TYPE");
            getColumnsDWColumns.put(22, "IS_AUTOINCREMENT");
            getColumnsDWColumns.put(21, "IS_GENERATEDCOLUMN");
            getColumnsDWColumns.put(19, "SS_IS_SPARSE");
            getColumnsDWColumns.put(20, "SS_IS_COLUMN_SET");
            getColumnsDWColumns.put(23, "SS_UDT_CATALOG_NAME");
            getColumnsDWColumns.put(24, "SS_UDT_SCHEMA_NAME");
            getColumnsDWColumns.put(25, "SS_UDT_ASSEMBLY_TYPE_NAME");
            getColumnsDWColumns.put(26, "SS_XML_SCHEMACOLLECTION_CATALOG_NAME");
            getColumnsDWColumns.put(27, "SS_XML_SCHEMACOLLECTION_SCHEMA_NAME");
            getColumnsDWColumns.put(28, "SS_XML_SCHEMACOLLECTION_NAME");

            try (ResultSet resultSet = databaseMetaData.getColumns(null, null, tableName, "%");) {
                ResultSetMetaData rsmd = resultSet.getMetaData();
                int rowCount = 0;
                while (resultSet.next()) {
                    for (int i = 1; i < rsmd.getColumnCount(); i++) {
                        assertEquals(rsmd.getColumnName(i), getColumnsDWColumns.values().toArray()[i - 1]);
                    }
                    rowCount++;
                }
                assertEquals(3, rowCount);
            }
        }
    }

    @Test
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xSQLv15)
    @Tag(Constants.xAzureSQLDB)
    @Tag(Constants.xAzureSQLMI)
    public void testGetImportedKeysDW() throws SQLException {
        // To get the actual DW database name.
        SQLServerDataSource ds = new SQLServerDataSource();
        updateDataSource(connectionString, ds);
        try (Connection conn = getConnection();) {
            DatabaseMetaData databaseMetaData = conn.getMetaData();

            getImportedKeysDWColumns = new LinkedHashMap<>();
            getImportedKeysDWColumns.put(1, "PKTABLE_CAT");
            getImportedKeysDWColumns.put(2, "PKTABLE_SCHEM");
            getImportedKeysDWColumns.put(3, "PKTABLE_NAME");
            getImportedKeysDWColumns.put(4, "PKCOLUMN_NAME");
            getImportedKeysDWColumns.put(5, "FKTABLE_CAT");
            getImportedKeysDWColumns.put(6, "FKTABLE_SCHEM");
            getImportedKeysDWColumns.put(7, "FKTABLE_NAME");
            getImportedKeysDWColumns.put(8, "FKCOLUMN_NAME");
            getImportedKeysDWColumns.put(9, "KEY_SEQ");
            getImportedKeysDWColumns.put(10, "UPDATE_RULE");
            getImportedKeysDWColumns.put(11, "DELETE_RULE");
            getImportedKeysDWColumns.put(12, "FK_NAME");
            getImportedKeysDWColumns.put(13, "PK_NAME");
            getImportedKeysDWColumns.put(14, "DEFERRABILITY");

            try (ResultSet resultSet = databaseMetaData.getImportedKeys(ds.getDatabaseName(), null, tableName);) {
                assertFalse(resultSet.next());
                ResultSetMetaData rsmd = resultSet.getMetaData();
                // Verify metadata
                for (int i = 1; i < getImportedKeysDWColumns.size(); i++) {
                    assertEquals(getImportedKeysDWColumns.get(i), rsmd.getColumnName(i));
                }

            }
        }
    }

    @BeforeAll
    public static void setupTable() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " ([col_1] int NOT NULL, [col%2] varchar(200), [col[3] decimal(15,2))");
            stmt.execute("CREATE FUNCTION " + AbstractSQLGenerator.escapeIdentifier(functionName)
                    + " (@p1 INT, @p2 INT) RETURNS INT AS BEGIN DECLARE @result INT; SET @result = @p1 + @p2; RETURN @result; END");
        }
    }

    @AfterAll
    public static void terminate() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropFunctionIfExists(functionName, stmt);
        }
    }
}
