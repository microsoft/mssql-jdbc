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
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
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
import com.microsoft.sqlserver.jdbc.SQLServerException;
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

    private static final String uuid = UUID.randomUUID().toString().replaceAll("-", "");
    private static final String tableName = RandomUtil.getIdentifier("DBMetadataTable");
    private static final String functionName = RandomUtil.getIdentifier("DBMetadataFunction");
    private static final String newUserName = "newUser" + uuid;
    private static final String schema = "schema_demo" + uuid;
    private static final String escapedSchema = "schema\\_demo" + uuid;
    private static final String tableNameWithSchema = schema + ".resource";
    private static final String sprocWithSchema = schema + ".updateresource";
    private static Map<Integer, String> getColumnsDWColumns = null;
    private static Map<Integer, String> getImportedKeysDWColumns = null;
    private static final String TABLE_CAT = "TABLE_CAT";
    private static final String TABLE_SCHEM = "TABLE_SCHEM";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String DATA_TYPE = "DATA_TYPE";
    private static final String TYPE_NAME = "TYPE_NAME";
    private static final String COLUMN_SIZE = "COLUMN_SIZE";
    private static final String BUFFER_LENGTH = "BUFFER_LENGTH";
    private static final String DECIMAL_DIGITS = "DECIMAL_DIGITS";
    private static final String NUM_PREC_RADIX = "NUM_PREC_RADIX";
    private static final String NULLABLE = "NULLABLE";
    private static final String REMARKS = "REMARKS";
    private static final String COLUMN_DEF = "COLUMN_DEF";
    private static final String SQL_DATA_TYPE = "SQL_DATA_TYPE";
    private static final String SQL_DATETIME_SUB = "SQL_DATETIME_SUB";
    private static final String CHAR_OCTET_LENGTH = "CHAR_OCTET_LENGTH";
    private static final String ORDINAL_POSITION = "ORDINAL_POSITION";
    private static final String IS_NULLABLE = "IS_NULLABLE";
    private static final String SCOPE_CATALOG = "SCOPE_CATALOG";
    private static final String SCOPE_SCHEMA = "SCOPE_SCHEMA";
    private static final String SCOPE_TABLE = "SCOPE_TABLE";
    private static final String SOURCE_DATA_TYPE = "SOURCE_DATA_TYPE";
    private static final String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
    private static final String IS_GENERATEDCOLUMN = "IS_GENERATEDCOLUMN";
    private static final String SS_IS_SPARSE = "SS_IS_SPARSE";
    private static final String SS_IS_COLUMN_SET = "SS_IS_COLUMN_SET";
    private static final String SS_UDT_CATALOG_NAME = "SS_UDT_CATALOG_NAME";
    private static final String SS_UDT_SCHEMA_NAME = "SS_UDT_SCHEMA_NAME";
    private static final String SS_UDT_ASSEMBLY_TYPE_NAME = "SS_UDT_ASSEMBLY_TYPE_NAME";
    private static final String SS_XML_SCHEMACOLLECTION_CATALOG_NAME = "SS_XML_SCHEMACOLLECTION_CATALOG_NAME";
    private static final String SS_XML_SCHEMACOLLECTION_SCHEMA_NAME = "SS_XML_SCHEMACOLLECTION_SCHEMA_NAME";
    private static final String SS_XML_SCHEMACOLLECTION_NAME = "SS_XML_SCHEMACOLLECTION_NAME";

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

    @Test
    public void testDatabaseCompatibilityLevel() throws SQLException {
        try (Connection conn = getConnection()) {
            SQLServerDatabaseMetaData dbmData = (SQLServerDatabaseMetaData) conn.getMetaData();
            int compatibilityLevel = dbmData.getDatabaseCompatibilityLevel();
            assertTrue(compatibilityLevel > 0);
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
    public void testDBUserLogin() throws SQLException {
        String auth = TestUtils.getProperty(connectionString, "authentication");
        org.junit.Assume.assumeTrue(auth != null
                && (auth.equalsIgnoreCase("SqlPassword") || auth.equalsIgnoreCase("ActiveDirectoryPassword")));

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
                    TestResource.getResource("R_userNameNotMatch") + "userName: " + userName + "from connectio string: "
                            + userFromConnectionString);
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testImpersonateGetUserName() throws SQLException {
        String escapedNewUser = AbstractSQLGenerator.escapeIdentifier(newUserName);

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            String password = "password" + UUID.randomUUID();

            TestUtils.dropUserIfExists(newUserName, stmt);
            TestUtils.dropLoginIfExists(newUserName, stmt);

            // create new user and login
            try {
                stmt.execute("CREATE USER " + escapedNewUser + " WITH password='" + password + "'");
            } catch (SQLServerException e) {
                // handle failed cases when database is master
                if (e.getMessage().contains("contained database")) {
                    stmt.execute("CREATE LOGIN " + escapedNewUser + " WITH password='" + password + "'");
                    stmt.execute("CREATE USER " + escapedNewUser);
                }
            }

            DatabaseMetaData databaseMetaData = conn.getMetaData();
            try (CallableStatement asOtherUser = conn.prepareCall("EXECUTE AS USER = '" + newUserName + "'")) {
                asOtherUser.execute();
                assertTrue(newUserName.equalsIgnoreCase(databaseMetaData.getUserName()),
                        TestResource.getResource("R_userNameNotMatch"));
            } catch (Exception e) {
                fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
            }
        } finally {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropUserIfExists(newUserName, stmt);
                TestUtils.dropLoginIfExists(newUserName, stmt);
            }
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
     * Tests that the schemaPattern parameter containing _ and % are escaped by
     * {@link SQLServerDatabaseMetaData#getSchemas(String catalog, String schemaPattern)}.
     *
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    public void testDBSchemasForSchemaPatternWithWildcards() throws SQLException {
        UUID id = UUID.randomUUID();
        String testCatalog = "catalog" + id;
        String[] schemas = {"some_schema", "some%schema", "some[schema"};
        String[] schemaPatterns = {"some\\_schema", "some\\%schema", "some\\[schema"};

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropDatabaseIfExists(testCatalog, connectionString);
            stmt.execute(String.format("CREATE DATABASE [%s]", testCatalog));
            stmt.execute(String.format("USE [%s]", testCatalog));

            for (int i = 0; i < schemas.length; ++i) {
                stmt.execute(String.format("CREATE SCHEMA [%s]", schemas[i]));

                try (ResultSet rs = conn.getMetaData().getSchemas(testCatalog, schemaPatterns[i])) {

                    MessageFormat schemaEmptyFormat = new MessageFormat(TestResource.getResource("R_nameEmpty"));
                    Object[] schemaMsgArgs = {schemas[i]};
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

                    MessageFormat atLeastOneFoundFormat = new MessageFormat(
                            TestResource.getResource("R_atLeastOneFound"));
                    assertTrue(hasResults, atLeastOneFoundFormat.format(schemaMsgArgs));
                }
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
                Object[] msgArgs1 = {"catalog"};
                assertTrue(rsCatalog.next(),
                        (new MessageFormat(TestResource.getResource("R_atLeastOneFound"))).format(msgArgs1));

                String dbNameFromCatalog = rsCatalog.getString("TABLE_CAT");
                String[] types = {"TABLE"};
                Object[] msgArgs2 = {"Table"};
                String dbNameFromConnectionString = TestUtils.getProperty(connectionString, "databaseName");
                if (null == dbNameFromConnectionString || (null != dbNameFromConnectionString
                        && dbNameFromConnectionString.equals(dbNameFromCatalog))) {
                    try (ResultSet rs = databaseMetaData.getTables(dbNameFromCatalog, null, "%", types)) {
                        while (rs.next()) {
                            assertTrue(!StringUtils.isEmpty(rs.getString("TABLE_NAME")),
                                    (new MessageFormat(TestResource.getResource("R_nameEmpty"))).format(msgArgs2));
                        }
                    }
                } else {
                    // try to find the databaseName specified
                    while (rsCatalog.next()) {
                        dbNameFromCatalog = rsCatalog.getString("TABLE_CAT");
                        if (null != dbNameFromCatalog && !dbNameFromCatalog.isEmpty()
                                && dbNameFromConnectionString.equals(dbNameFromCatalog)) {
                            try (ResultSet rs = databaseMetaData.getTables(dbNameFromCatalog, null, "%", types)) {
                                while (rs.next()) {
                                    assertTrue(!StringUtils.isEmpty(rs.getString("TABLE_NAME")),
                                            (new MessageFormat(TestResource.getResource("R_nameEmpty")))
                                                    .format(msgArgs2));
                                }
                                return;
                            }
                        }
                    }

                    Object[] msgArgs3 = {dbNameFromConnectionString};
                    fail((new MessageFormat(TestResource.getResource("R_databaseNotFound"))).format(msgArgs3));
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
    public void testGetColumns() throws SQLException {
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

            Map<String, Object> firstRow = new HashMap<String, Object>();
            String dbName = ds.getDatabaseName();
            if (null == dbName) {
                firstRow.put("TABLE_CAT", "master");
            } else {
                firstRow.put("TABLE_CAT", ds.getDatabaseName());
            }
            firstRow.put("TABLE_SCHEM", "dbo");
            firstRow.put("TABLE_NAME", tableName);
            firstRow.put("COLUMN_NAME", "col_1");
            firstRow.put("DATA_TYPE", 4);
            firstRow.put("TYPE_NAME", "int");
            firstRow.put("COLUMN_SIZE", 10);
            firstRow.put("BUFFER_LENGTH", 4);
            firstRow.put("DECIMAL_DIGITS", 0);
            firstRow.put("NUM_PREC_RADIX", 10);
            firstRow.put("NULLABLE", 0);
            firstRow.put("REMARKS", null);
            firstRow.put("COLUMN_DEF", null);
            firstRow.put("SQL_DATA_TYPE", 4);
            firstRow.put("SQL_DATETIME_SUB", null);
            firstRow.put("CHAR_OCTET_LENGTH", null);
            firstRow.put("ORDINAL_POSITION", 1);
            firstRow.put("IS_NULLABLE", "NO");
            firstRow.put("SCOPE_CATALOG", null);
            firstRow.put("SCOPE_SCHEMA", null);
            firstRow.put("SCOPE_TABLE", null);
            firstRow.put("SOURCE_DATA_TYPE", 56);
            firstRow.put("IS_AUTOINCREMENT", "NO");
            firstRow.put("IS_GENERATEDCOLUMN", "NO");
            firstRow.put("SS_IS_SPARSE", 0);
            firstRow.put("SS_IS_COLUMN_SET", 0);
            firstRow.put("SS_UDT_CATALOG_NAME", null);
            firstRow.put("SS_UDT_SCHEMA_NAME", null);
            firstRow.put("SS_UDT_ASSEMBLY_TYPE_NAME", null);
            firstRow.put("SS_XML_SCHEMACOLLECTION_CATALOG_NAME", null);
            firstRow.put("SS_XML_SCHEMACOLLECTION_SCHEMA_NAME", null);
            firstRow.put("SS_XML_SCHEMACOLLECTION_NAME", null);

            try (ResultSet resultSet = databaseMetaData.getColumns(null, null, tableName, "%");) {
                ResultSetMetaData rsmd = resultSet.getMetaData();
                int rowCount = 0;
                while (resultSet.next()) {
                    for (int i = 1; i < rsmd.getColumnCount(); i++) {
                        String columnName = rsmd.getColumnName(i);
                        Object value = resultSet.getObject(columnName);
                        if (0 == rowCount) {
                            int expectedType = rsmd.getColumnType(i);
                            if (null != firstRow.get(columnName)
                                    && (Types.VARCHAR == expectedType || Types.NVARCHAR == expectedType)) {
                                assertEquals(firstRow.get(columnName).toString().toLowerCase(),
                                        resultSet.getString(columnName).toLowerCase());
                            } else if (null != firstRow.get(columnName) && (Types.TINYINT == expectedType
                                    || Types.SMALLINT == expectedType || Types.INTEGER == expectedType)) {
                                assertEquals(firstRow.get(columnName), resultSet.getInt(columnName));
                            } else {
                                assertEquals(firstRow.get(columnName), value);
                            }
                        }
                        assertEquals(getColumnsDWColumns.values().toArray()[i - 1], columnName);
                    }
                    rowCount++;
                }
                assertEquals(3, rowCount);
            }
        }
    }

    @Test
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xSQLv15)
    @Tag(Constants.xSQLv16)
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

    /**
     * Validates the metadata data types defined by JDBC spec.
     * Refer to <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getColumns-java.lang.String-java.lang.String-java.lang.String-java.lang.String-">DatabaseMetadata getColumns() specs</a>
     * 
     * @throws SQLException
     */
    @Test
    public void testValidateColumnMetadata() throws SQLException {
        Map<String, Class<?>> getColumnMetaDataClass = new LinkedHashMap<>();

        getColumnMetaDataClass.put(TABLE_CAT, String.class);
        getColumnMetaDataClass.put(TABLE_SCHEM, String.class);
        getColumnMetaDataClass.put(TABLE_NAME, String.class);
        getColumnMetaDataClass.put(COLUMN_NAME, String.class);
        getColumnMetaDataClass.put(DATA_TYPE, Integer.class);
        getColumnMetaDataClass.put(TYPE_NAME, String.class);
        getColumnMetaDataClass.put(COLUMN_SIZE, Integer.class);
        getColumnMetaDataClass.put(BUFFER_LENGTH, Integer.class); // Not used
        getColumnMetaDataClass.put(DECIMAL_DIGITS, Integer.class);
        getColumnMetaDataClass.put(NUM_PREC_RADIX, Integer.class);
        getColumnMetaDataClass.put(NULLABLE, Integer.class);
        getColumnMetaDataClass.put(REMARKS, String.class);
        getColumnMetaDataClass.put(COLUMN_DEF, String.class);
        getColumnMetaDataClass.put(SQL_DATA_TYPE, Integer.class);
        getColumnMetaDataClass.put(SQL_DATETIME_SUB, Integer.class);
        getColumnMetaDataClass.put(CHAR_OCTET_LENGTH, Integer.class);
        getColumnMetaDataClass.put(ORDINAL_POSITION, Integer.class);
        getColumnMetaDataClass.put(IS_NULLABLE, String.class);
        getColumnMetaDataClass.put(SCOPE_CATALOG, String.class);
        getColumnMetaDataClass.put(SCOPE_SCHEMA, String.class);
        getColumnMetaDataClass.put(SCOPE_TABLE, String.class);
        getColumnMetaDataClass.put(SOURCE_DATA_TYPE, Short.class);
        getColumnMetaDataClass.put(IS_AUTOINCREMENT, String.class);
        getColumnMetaDataClass.put(IS_GENERATEDCOLUMN, String.class);
        getColumnMetaDataClass.put(SS_IS_SPARSE, Short.class);
        getColumnMetaDataClass.put(SS_IS_COLUMN_SET, Short.class);
        getColumnMetaDataClass.put(SS_UDT_CATALOG_NAME, String.class);
        getColumnMetaDataClass.put(SS_UDT_SCHEMA_NAME, String.class);
        getColumnMetaDataClass.put(SS_UDT_ASSEMBLY_TYPE_NAME, String.class);
        getColumnMetaDataClass.put(SS_XML_SCHEMACOLLECTION_CATALOG_NAME, String.class);
        getColumnMetaDataClass.put(SS_XML_SCHEMACOLLECTION_SCHEMA_NAME, String.class);
        getColumnMetaDataClass.put(SS_XML_SCHEMACOLLECTION_NAME, String.class);

        try (Connection conn = getConnection()) {
            ResultSetMetaData metadata = conn.getMetaData().getColumns(null, null, tableName, null).getMetaData();

            // Ensure that there is an expected class for every column in the metadata result set
            assertEquals(metadata.getColumnCount(), getColumnMetaDataClass.size());

            for (int i = 1; i < metadata.getColumnCount(); i++) {
                String columnLabel = metadata.getColumnLabel(i);
                String columnClassName = metadata.getColumnClassName(i);
                Class<?> expectedClass = getColumnMetaDataClass.get(columnLabel);

                // Ensure the metadata column is in the metadata column class map
                if (expectedClass == null) {
                    MessageFormat form1 = new MessageFormat(TestResource.getResource("R_objectNullOrEmpty"));
                    Object[] msgArgs1 = {"expected metadata column class for column " + columnLabel};
                    fail(form1.format(msgArgs1));
                }

                // Ensure the actual and expected column metadata types match
                if (!columnClassName.equals(expectedClass.getName())) {
                    MessageFormat form1 = new MessageFormat(
                            TestResource.getResource("R_expectedClassDoesNotMatchActualClass"));
                    Object[] msgArgs1 = {expectedClass.getName(), columnClassName, columnLabel};
                    fail(form1.format(msgArgs1));
                }
            }
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void shouldEscapeSchemaName() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE SCHEMA " + schema);
            stmt.execute("CREATE TABLE " + tableNameWithSchema + " (id UNIQUEIDENTIFIER, name NVARCHAR(400));");
            stmt.execute("CREATE PROCEDURE " + sprocWithSchema + "(@id UNIQUEIDENTIFIER, @name VARCHAR(400)) AS "
                    + "BEGIN SET TRANSACTION ISOLATION LEVEL SERIALIZABLE BEGIN TRANSACTION UPDATE "
                    + tableNameWithSchema + " SET name = @name WHERE id = @id COMMIT END");
        }

        try (Connection con = getConnection()) {
            DatabaseMetaData md = con.getMetaData();
            try (ResultSet procedures = md.getProcedures(null, escapedSchema, "updateresource")) {
                if (!procedures.next()) {
                    fail("Escaped schema pattern did not succeed. No results found.");
                }
            }

            try (ResultSet columns = md.getProcedureColumns(null, escapedSchema, "updateresource", null)) {
                if (!columns.next()) {
                    fail("Escaped schema pattern did not succeed. No results found.");
                }
            }
        }

        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableWithSchemaIfExists(tableNameWithSchema, stmt);
            TestUtils.dropProcedureWithSchemaIfExists(sprocWithSchema, stmt);
            TestUtils.dropSchemaIfExists(schema, stmt);
        }
    }

    @BeforeAll
    public static void setupTable() throws Exception {
        setConnection();

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
            TestUtils.dropTableWithSchemaIfExists(tableNameWithSchema, stmt);
            TestUtils.dropProcedureWithSchemaIfExists(sprocWithSchema, stmt);
            TestUtils.dropSchemaIfExists(schema, stmt);
        }
    }
}
