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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
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

    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    public void testGetSchemasWithAndWithoutCatalog() throws SQLException {
        UUID id = UUID.randomUUID();
        String dbName = "GetSchemas" + id;
        String schemaName = "TestSchema" + id;
        String[] constSchemas = {
            "dbo", "guest", "INFORMATION_SCHEMA", "sys", "db_owner", "db_accessadmin",
            "db_securityadmin", "db_ddladmin", "db_backupoperator", "db_datareader",
            "db_datawriter", "db_denydatareader", "db_denydatawriter"
        };

        try (Connection connection = getConnection();
            Statement stmt = connection.createStatement()) {
            TestUtils.dropDatabaseIfExists(dbName, connectionString);
            stmt.execute(String.format("CREATE DATABASE [%s]", dbName));
            stmt.execute(String.format("USE [%s]", dbName));
            stmt.execute(String.format("CREATE SCHEMA [%s]", schemaName));

            ResultSet rs = connection.getMetaData().getSchemas(dbName, null );
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEM");
                String catalog = rs.getString("TABLE_CATALOG");

                // When catalog is specified, all results should have non-null catalog
                assertNotNull(catalog, "TABLE_CATALOG should not be null for schema '" + schema + "' when catalog is specified");
            }

            rs = connection.getMetaData().getSchemas(null, null);
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEM");
                String catalog = rs.getString("TABLE_CATALOG");

                if (catalog == null) {
                    assertTrue(
                        Arrays.asList(constSchemas).contains(schema),
                        "Unexpected schema with null catalog: " + schema
                    );
                }
            }
        } finally {
            TestUtils.dropDatabaseIfExists(dbName, connectionString);
        }
    }
    
    /**
     * Test for VECTOR column metadata
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.vectorTest)
    public void testVectorMetaData() throws SQLException {
        String vectorTableName = RandomUtil.getIdentifier("vectorTable");

        try (Statement stmt = connection.createStatement()) {
            // Create a table with a VECTOR column
            String sql = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(vectorTableName)
                    + " (c1 VECTOR(3) NULL);";
            stmt.execute(sql);

            // Query the table and retrieve metadata
            String query = "SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(vectorTableName);
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(query)) {

                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                assertEquals(1, columnCount, "Column count should be 1");

                // Validate column name
                String columnName = metaData.getColumnName(1);
                assertEquals("c1", columnName, "Column name should be 'c1'");

                // Validate column type name
                String columnType = metaData.getColumnTypeName(1);
                assertTrue("VECTOR".equalsIgnoreCase(columnType), "Column type should be 'VECTOR'");

                // Validate column type
                int columnTypeInt = metaData.getColumnType(1);
                assertEquals(microsoft.sql.Types.VECTOR, columnTypeInt,
                        "Column type should be microsoft.sql.Types.VECTOR");

                // Validate column display size
                int columnDisplaySize = metaData.getColumnDisplaySize(1);
                assertTrue(columnDisplaySize > 0, "Column display size should be greater than 0");

                // Validate column precision
                int columnPrecision = metaData.getPrecision(1);
                assertEquals(3, columnPrecision, "Column precision should be same as dimensionCount");

                // Validate column scale
                int columnScale = metaData.getScale(1);
                assertEquals(4, columnScale, "Column scale should be 4");

                // Validate column is searchable
                boolean columnSearchable = metaData.isSearchable(1);
                assertFalse(columnSearchable, "Column should be non-searchable");

                // Validate column class name
                String columnClassName = metaData.getColumnClassName(1);
                assertEquals(microsoft.sql.Vector.class.getName(), columnClassName,
                        "Column class name should be 'microsoft.sql.Vector'");
            }
        } finally {
            // Cleanup: Drop the table
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + AbstractSQLGenerator.escapeIdentifier(vectorTableName));
            }
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
        try (Connection connection = getConnection(); Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropFunctionIfExists(functionName, stmt);
            TestUtils.dropTableWithSchemaIfExists(tableNameWithSchema, stmt);
            TestUtils.dropProcedureWithSchemaIfExists(sprocWithSchema, stmt);
            TestUtils.dropSchemaIfExists(schema, stmt);
        }
    }

    @Nested
    public class DatabaseMetadataGetIndexInfoTest extends AbstractTest {
        String tableName = AbstractSQLGenerator.escapeIdentifier("DBMetadataTestTable");
        String col1Name = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("col1"));
        String col2Name = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("col2"));
        String col3Name = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("col3"));

        @BeforeEach
        public void init() throws SQLException {
            try (Connection con = getConnection()) {
                con.setAutoCommit(false);
                try (Statement stmt = con.createStatement()) {
                    TestUtils.dropTableIfExists(tableName, stmt);
                    String createTableSQL = "CREATE TABLE " + tableName + " (" + col1Name + " INT, " + col2Name
                            + " INT, "
                            + col3Name + " INT)";

                    stmt.executeUpdate(createTableSQL);
                    assertNull(connection.getWarnings(),
                            TestResource.getResource("R_noSQLWarningsCreateTableConnection"));
                    assertNull(stmt.getWarnings(), TestResource.getResource("R_noSQLWarningsCreateTableStatement"));

                    String createClusteredIndexSQL = "CREATE CLUSTERED INDEX IDX_Clustered ON " + tableName + "("
                            + col1Name
                            + ")";
                    stmt.executeUpdate(createClusteredIndexSQL);
                    assertNull(connection.getWarnings(),
                            TestResource.getResource("R_noSQLWarningsCreateIndexConnection"));
                    assertNull(stmt.getWarnings(), TestResource.getResource("R_noSQLWarningsCreateIndexStatement"));

                    String createNonClusteredIndexSQL = "CREATE NONCLUSTERED INDEX IDX_NonClustered ON " + tableName
                            + "("
                            + col2Name + ")";
                    stmt.executeUpdate(createNonClusteredIndexSQL);
                    assertNull(connection.getWarnings(),
                            TestResource.getResource("R_noSQLWarningsCreateIndexConnection"));
                    assertNull(stmt.getWarnings(), TestResource.getResource("R_noSQLWarningsCreateIndexStatement"));

                    String createColumnstoreIndexSQL = "CREATE COLUMNSTORE INDEX IDX_Columnstore ON " + tableName + "("
                            + col3Name + ")";
                    stmt.executeUpdate(createColumnstoreIndexSQL);
                    assertNull(connection.getWarnings(),
                            TestResource.getResource("R_noSQLWarningsCreateIndexConnection"));
                    assertNull(stmt.getWarnings(), TestResource.getResource("R_noSQLWarningsCreateIndexStatement"));
                }
                con.commit();
            }
        }

        @AfterEach
        public void terminate() throws SQLException {
            try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
                try {
                    TestUtils.dropTableIfExists(tableName, stmt);
                } catch (SQLException e) {
                    fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
                }
            }
        }

        @Test
        public void testGetIndexInfo() throws SQLException {
            ResultSet rs1 = null;
            try (Connection connection = getConnection()) {
                String catalog = connection.getCatalog();
                String schema = "dbo";
                String table = "DBMetadataTestTable";
                DatabaseMetaData dbMetadata = connection.getMetaData();
                rs1 = dbMetadata.getIndexInfo(catalog, schema, table, false, false);

                boolean hasClusteredIndex = false;
                boolean hasNonClusteredIndex = false;
                boolean hasColumnstoreIndex = false;

                while (rs1.next()) {
                    String indexName = rs1.getString("INDEX_NAME");

                    if (indexName != null && indexName.contains("Columnstore")) {
                        hasColumnstoreIndex = true;
                    } else if (indexName != null && indexName.contains("NonClustered")) {
                        hasNonClusteredIndex = true;
                    } else if (indexName != null && indexName.contains("Clustered")) {
                        hasClusteredIndex = true;
                    }
                }

                // Verify that the expected indexes are present
                assertTrue(hasColumnstoreIndex, "COLUMNSTORE index not found.");
                assertTrue(hasClusteredIndex, "CLUSTERED index not found.");
                assertTrue(hasNonClusteredIndex, "NONCLUSTERED index not found.");
            }
        }

        @Test
        public void testGetIndexInfoCaseSensitivity() throws SQLException {
            ResultSet rs1, rs2 = null;
            try (Connection connection = getConnection()) {
                String catalog = connection.getCatalog();
                String schema = "dbo";
                String table = "DBMetadataTestTable";

                DatabaseMetaData dbMetadata = connection.getMetaData();
                rs1 = dbMetadata.getIndexInfo(catalog, schema, table, false, false);
                rs2 = dbMetadata.getIndexInfo(catalog, schema, table.toUpperCase(), false, false);

                while (rs1.next() && rs2.next()) {
                    String indexType = rs1.getString("TYPE");
                    String indexName = rs1.getString("INDEX_NAME");
                    String catalogName = rs1.getString("TABLE_CAT");
                    String schemaName = rs1.getString("TABLE_SCHEM");
                    String tableName = rs1.getString("TABLE_NAME");
                    boolean isUnique = rs1.getBoolean("NON_UNIQUE");
                    String columnName = rs1.getString("COLUMN_NAME");
                    int columnOrder = rs1.getInt("ORDINAL_POSITION");

                    assertEquals(catalogName, rs2.getString("TABLE_CAT"));
                    assertEquals(schemaName, rs2.getString("TABLE_SCHEM"));
                    assertEquals(tableName, rs2.getString("TABLE_NAME"));
                    assertEquals(indexName, rs2.getString("INDEX_NAME"));
                    assertEquals(indexType, rs2.getString("TYPE"));
                    assertEquals(isUnique, rs2.getBoolean("NON_UNIQUE"));
                    assertEquals(columnName, rs2.getString("COLUMN_NAME"));
                    assertEquals(columnOrder, rs2.getInt("ORDINAL_POSITION"));
                }
            }
        }
    

    @Test
    public void testDatabaseCapabilityMethods() throws SQLException {
        try (Connection con = getConnection()) {
            DatabaseMetaData dmd = con.getMetaData();

            assertTrue(dmd.allProceduresAreCallable(), "All procedures should be callable");

            assertTrue(dmd.allTablesAreSelectable(), "All tables should be selectable");

            assertFalse(dmd.autoCommitFailureClosesAllResultSets(),
                    "Auto commit failure should not close all result sets");

            assertFalse(dmd.dataDefinitionCausesTransactionCommit(),
                    "Data definition should not cause transaction commit");

            assertFalse(dmd.dataDefinitionIgnoredInTransactions(),
                    "Data definition should not be ignored in transactions");

            assertFalse(dmd.doesMaxRowSizeIncludeBlobs(), "Max row size should not include blobs");

            assertTrue(dmd.generatedKeyAlwaysReturned(), "Generated key should always be returned");

            assertEquals(2147483647L, dmd.getMaxLogicalLobSize(), "Max logical LOB size should be 2147483647");

            assertFalse(dmd.supportsRefCursors(), "Should not support ref cursors");

            assertFalse(dmd.supportsSharding(), "Should not support sharding");

            assertEquals(".", dmd.getCatalogSeparator(), "Catalog separator should be '.'");

            assertEquals("database", dmd.getCatalogTerm(), "Catalog term should be 'database'");
        }
    }

    @Test
    public void testDatabaseMetaDataMethodsCodeCoverage() throws Exception {
        // Create a mock SQLServerDatabaseMetaData with real connection for basic functionality
        try (Connection conn = getConnection()) {
            SQLServerDatabaseMetaData dmd = (SQLServerDatabaseMetaData) conn.getMetaData();

            String productVersion = dmd.getDatabaseProductVersion();
            assertNotNull(productVersion);

            int defaultIsolation = dmd.getDefaultTransactionIsolation();
            assertEquals(Connection.TRANSACTION_READ_COMMITTED, defaultIsolation);

            String extraNameChars = dmd.getExtraNameCharacters();
            assertEquals("$#@", extraNameChars);

            String quoteString = dmd.getIdentifierQuoteString();
            assertEquals("\"", quoteString);

            String procedureTerm = dmd.getProcedureTerm();
            assertEquals("stored procedure", procedureTerm);

            String schemaTerm = dmd.getSchemaTerm();
            assertEquals("schema", schemaTerm);

            String searchEscape = dmd.getSearchStringEscape();
            assertEquals("\\", searchEscape);

            String sqlKeywords = dmd.getSQLKeywords();
            assertNotNull(sqlKeywords);
            assertTrue(sqlKeywords.length() > 0);

            String stringFunctions = dmd.getStringFunctions();
            assertNotNull(stringFunctions);
            assertTrue(stringFunctions.contains("ASCII"));

            String systemFunctions = dmd.getSystemFunctions();
            assertNotNull(systemFunctions);
            assertTrue(systemFunctions.contains("DATABASE"));

            String timeDateFunctions = dmd.getTimeDateFunctions();
            assertNotNull(timeDateFunctions);
            assertTrue(timeDateFunctions.contains("CURDATE"));

            // Test catalog/schema support methods
            assertTrue(dmd.isCatalogAtStart());
            assertFalse(dmd.isReadOnly());
            assertTrue(dmd.nullPlusNonNullIsNull());

            // Test null sorting methods
            assertFalse(dmd.nullsAreSortedAtEnd());
            assertFalse(dmd.nullsAreSortedAtStart());
            assertFalse(dmd.nullsAreSortedHigh());
            assertTrue(dmd.nullsAreSortedLow());

            // Test identifier storage methods
            assertFalse(dmd.storesLowerCaseIdentifiers());
            assertFalse(dmd.storesLowerCaseQuotedIdentifiers());
            assertTrue(dmd.storesMixedCaseIdentifiers());
            assertTrue(dmd.storesMixedCaseQuotedIdentifiers());
            assertFalse(dmd.storesUpperCaseIdentifiers());
            assertFalse(dmd.storesUpperCaseQuotedIdentifiers());

            // Test SQL feature support methods
            assertTrue(dmd.supportsAlterTableWithAddColumn());
            assertTrue(dmd.supportsAlterTableWithDropColumn());
            assertTrue(dmd.supportsANSI92EntryLevelSQL());
            assertFalse(dmd.supportsANSI92FullSQL());
            assertFalse(dmd.supportsANSI92IntermediateSQL());

            // Test catalog support methods
            assertTrue(dmd.supportsCatalogsInDataManipulation());
            assertTrue(dmd.supportsCatalogsInIndexDefinitions());
            assertTrue(dmd.supportsCatalogsInPrivilegeDefinitions());
            assertTrue(dmd.supportsCatalogsInProcedureCalls());
            assertTrue(dmd.supportsCatalogsInTableDefinitions());

            // Test column and conversion support
            assertTrue(dmd.supportsColumnAliasing());
            assertTrue(dmd.supportsConvert());
            assertTrue(dmd.supportsConvert(Types.INTEGER, Types.VARCHAR));

            // Test SQL grammar support
            assertTrue(dmd.supportsCoreSQLGrammar());
            assertTrue(dmd.supportsCorrelatedSubqueries());
            assertTrue(dmd.supportsDataDefinitionAndDataManipulationTransactions());
            assertFalse(dmd.supportsDataManipulationTransactionsOnly());
            assertFalse(dmd.supportsDifferentTableCorrelationNames());

            // Test expression and join support
            assertTrue(dmd.supportsExpressionsInOrderBy());
            assertFalse(dmd.supportsExtendedSQLGrammar());
            assertTrue(dmd.supportsFullOuterJoins());

            // Test GROUP BY support
            assertTrue(dmd.supportsGroupBy());
            assertTrue(dmd.supportsGroupByBeyondSelect());
            assertTrue(dmd.supportsGroupByUnrelated());

            // Test misc feature support
            assertFalse(dmd.supportsIntegrityEnhancementFacility());
            assertTrue(dmd.supportsLikeEscapeClause());
            assertTrue(dmd.supportsLimitedOuterJoins());
            assertTrue(dmd.supportsMinimumSQLGrammar());
            assertTrue(dmd.supportsMixedCaseIdentifiers());
            assertTrue(dmd.supportsMixedCaseQuotedIdentifiers());

            try (ResultSet tableTypes = dmd.getTableTypes()) {
                assertNotNull(tableTypes);
                boolean hasTable = false;
                while (tableTypes.next()) {
                    String tableType = tableTypes.getString("TABLE_TYPE");
                    if ("TABLE".equals(tableType)) {
                        hasTable = true;
                    }
                }
                assertTrue(hasTable);
            }

            try (ResultSet pseudoCols = dmd.getPseudoColumns(null, null, null, null)) {
                assertNotNull(pseudoCols);
                // Should return empty result set for SQL Server
                assertFalse(pseudoCols.next());
            }

            try (ResultSet typeInfo = dmd.getTypeInfo()) {
                assertNotNull(typeInfo);
                assertTrue(typeInfo.next()); // Should have at least one type
            }

            assertEquals(0, dmd.getMaxBinaryLiteralLength());
            assertEquals(128, dmd.getMaxCatalogNameLength());
            assertEquals(0, dmd.getMaxCharLiteralLength());
            assertEquals(128, dmd.getMaxColumnNameLength());
            assertEquals(0, dmd.getMaxColumnsInGroupBy());
            assertEquals(16, dmd.getMaxColumnsInIndex());
            assertEquals(0, dmd.getMaxColumnsInOrderBy());
            assertEquals(4096, dmd.getMaxColumnsInSelect());
            assertEquals(1024, dmd.getMaxColumnsInTable());
            assertEquals(0, dmd.getMaxCursorNameLength());
            assertEquals(900, dmd.getMaxIndexLength());
            assertEquals(128, dmd.getMaxProcedureNameLength());
            assertEquals(8060, dmd.getMaxRowSize());
            assertEquals(128, dmd.getMaxSchemaNameLength());
            assertEquals(524288000, dmd.getMaxStatementLength());
            assertEquals(0, dmd.getMaxStatements());
            assertEquals(128, dmd.getMaxTableNameLength());
            assertEquals(256, dmd.getMaxTablesInSelect());
            assertEquals(128, dmd.getMaxUserNameLength());

            // Test function string methods
            String numericFunctions = dmd.getNumericFunctions();
            assertNotNull(numericFunctions);
        }
    }

    @Test
    public void testDatabaseProductNameDriverNameAndSupportMethods() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();

            // Test getDatabaseProductName()
            String productName = dbmd.getDatabaseProductName();
            assertEquals("Microsoft SQL Server", productName, "Database product name should be 'Microsoft SQL Server'");

            // Test getDriverName()
            String driverName = dbmd.getDriverName();
            assertNotNull(driverName, "Driver name should not be null");
            assertTrue(driverName.contains("Microsoft"), "Driver name should contain 'Microsoft'");

            // Test support methods that return true (lines 2196-2375)
            assertTrue(dbmd.supportsMultipleResultSets(), "Should support multiple result sets");
            assertTrue(dbmd.supportsMultipleTransactions(), "Should support multiple transactions");
            assertTrue(dbmd.supportsNonNullableColumns(), "Should support non-nullable columns");
            assertTrue(dbmd.supportsOpenStatementsAcrossCommit(), "Should support open statements across commit");
            assertTrue(dbmd.supportsOpenStatementsAcrossRollback(), "Should support open statements across rollback");
            assertTrue(dbmd.supportsOrderByUnrelated(), "Should support ORDER BY unrelated");
            assertTrue(dbmd.supportsOuterJoins(), "Should support outer joins");
            assertTrue(dbmd.supportsPositionedDelete(), "Should support positioned delete");
            assertTrue(dbmd.supportsPositionedUpdate(), "Should support positioned update");
            assertTrue(dbmd.supportsSchemasInDataManipulation(), "Should support schemas in data manipulation");
            assertTrue(dbmd.supportsSchemasInIndexDefinitions(), "Should support schemas in index definitions");
            assertTrue(dbmd.supportsSchemasInPrivilegeDefinitions(), "Should support schemas in privilege definitions");
            assertTrue(dbmd.supportsSchemasInProcedureCalls(), "Should support schemas in procedure calls");
            assertTrue(dbmd.supportsSchemasInTableDefinitions(), "Should support schemas in table definitions");
            assertTrue(dbmd.supportsStoredProcedures(), "Should support stored procedures");
            assertTrue(dbmd.supportsSubqueriesInComparisons(), "Should support subqueries in comparisons");
            assertTrue(dbmd.supportsSubqueriesInExists(), "Should support subqueries in EXISTS");
            assertTrue(dbmd.supportsSubqueriesInIns(), "Should support subqueries in IN clauses");
            assertTrue(dbmd.supportsSubqueriesInQuantifieds(), "Should support subqueries in quantified expressions");
            assertTrue(dbmd.supportsTableCorrelationNames(), "Should support table correlation names");
            assertTrue(dbmd.supportsUnion(), "Should support UNION");
            assertTrue(dbmd.supportsUnionAll(), "Should support UNION ALL");

            // Test support methods that return false
            assertFalse(dbmd.supportsOpenCursorsAcrossCommit(), "Should not support open cursors across commit");
            assertFalse(dbmd.supportsOpenCursorsAcrossRollback(), "Should not support open cursors across rollback");
            assertFalse(dbmd.supportsSelectForUpdate(), "Should not support SELECT FOR UPDATE");
            assertFalse(dbmd.usesLocalFilePerTable(), "Should not use local file per table");
            assertFalse(dbmd.usesLocalFiles(), "Should not use local files");

            // Test supportsTransactionIsolationLevel with valid isolation levels
            assertTrue(dbmd.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED),
                    "Should support READ_UNCOMMITTED isolation level");
            assertTrue(dbmd.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED),
                    "Should support READ_COMMITTED isolation level");
            assertTrue(dbmd.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ),
                    "Should support REPEATABLE_READ isolation level");
            assertTrue(dbmd.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE),
                    "Should support SERIALIZABLE isolation level");

            // Test SQLServerConnection.TRANSACTION_SNAPSHOT if available
            try {
                Class<?> sqlServerConnClass = Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection");
                java.lang.reflect.Field snapshotField = sqlServerConnClass.getField("TRANSACTION_SNAPSHOT");
                int snapshotLevel = snapshotField.getInt(null);
                assertTrue(dbmd.supportsTransactionIsolationLevel(snapshotLevel),
                        "Should support SNAPSHOT isolation level");
            } catch (Exception e) {
                // TRANSACTION_SNAPSHOT field might not be accessible, skip this test
            }

            // Test supportsTransactionIsolationLevel with invalid isolation level
            assertFalse(dbmd.supportsTransactionIsolationLevel(999), "Should not support invalid isolation level");

            // Test supportsTransactions() - delegates to connection
            // This should generally return true for SQL Server connections
            boolean supportsTransactions = dbmd.supportsTransactions();
            // We don't assert a specific value since it depends on connection configuration
            // but we ensure the method doesn't throw an exception
            assertNotNull(supportsTransactions);
        }
    }

    @Test
    public void testResultSetCapabilitiesAndJDBCVersionMethods() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();

            // Test supportsResultSetType with valid types
            assertTrue(dbmd.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY), "Should support TYPE_FORWARD_ONLY");
            assertTrue(dbmd.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE),
                    "Should support TYPE_SCROLL_INSENSITIVE");
            assertTrue(dbmd.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE),
                    "Should support TYPE_SCROLL_SENSITIVE");

            // Test SQL Server specific result set types
            assertTrue(dbmd.supportsResultSetType(2003), // TYPE_SS_DIRECT_FORWARD_ONLY
                    "Should support TYPE_SS_DIRECT_FORWARD_ONLY");
            assertTrue(dbmd.supportsResultSetType(2004), // TYPE_SS_SERVER_CURSOR_FORWARD_ONLY
                    "Should support TYPE_SS_SERVER_CURSOR_FORWARD_ONLY");
            assertTrue(dbmd.supportsResultSetType(1006), // TYPE_SS_SCROLL_DYNAMIC
                    "Should support TYPE_SS_SCROLL_DYNAMIC");

            // Test supportsResultSetConcurrency
            assertTrue(dbmd.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY),
                    "Should support FORWARD_ONLY with READ_ONLY");
            assertTrue(dbmd.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE),
                    "Should support FORWARD_ONLY with UPDATABLE");
            assertTrue(dbmd.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY),
                    "Should support SCROLL_SENSITIVE with READ_ONLY");
            assertTrue(dbmd.supportsResultSetConcurrency(1006, ResultSet.CONCUR_UPDATABLE), // TYPE_SS_SCROLL_DYNAMIC
                    "Should support TYPE_SS_SCROLL_DYNAMIC with UPDATABLE");

            // Test SCROLL_INSENSITIVE only supports READ_ONLY
            assertTrue(dbmd.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY),
                    "Should support SCROLL_INSENSITIVE with READ_ONLY");
            assertFalse(
                    dbmd.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE),
                    "Should not support SCROLL_INSENSITIVE with UPDATABLE");

            // Test SS_DIRECT_FORWARD_ONLY only supports READ_ONLY
            assertTrue(dbmd.supportsResultSetConcurrency(2003, ResultSet.CONCUR_READ_ONLY), // TYPE_SS_DIRECT_FORWARD_ONLY
                    "Should support TYPE_SS_DIRECT_FORWARD_ONLY with READ_ONLY");
            assertFalse(dbmd.supportsResultSetConcurrency(2003, ResultSet.CONCUR_UPDATABLE), // TYPE_SS_DIRECT_FORWARD_ONLY
                    "Should not support TYPE_SS_DIRECT_FORWARD_ONLY with UPDATABLE");

            // Test visibility methods for supported types
            int[] supportedTypes = {ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_SENSITIVE, 1006, // TYPE_SS_SCROLL_DYNAMIC
                    1005, // TYPE_SS_SCROLL_KEYSET
                    2004 // TYPE_SS_SERVER_CURSOR_FORWARD_ONLY
            };

            for (int type : supportedTypes) {
                assertTrue(dbmd.ownUpdatesAreVisible(type), "Own updates should be visible for type: " + type);
                assertTrue(dbmd.ownDeletesAreVisible(type), "Own deletes should be visible for type: " + type);
                assertTrue(dbmd.ownInsertsAreVisible(type), "Own inserts should be visible for type: " + type);
                assertTrue(dbmd.othersUpdatesAreVisible(type), "Others updates should be visible for type: " + type);
                assertTrue(dbmd.othersDeletesAreVisible(type), "Others deletes should be visible for type: " + type);
            }

            // Test othersInsertsAreVisible - only specific types support this
            int[] insertsVisibleTypes = {ResultSet.TYPE_FORWARD_ONLY, 1006, // TYPE_SS_SCROLL_DYNAMIC
                    2004 // TYPE_SS_SERVER_CURSOR_FORWARD_ONLY
            };

            for (int type : insertsVisibleTypes) {
                assertTrue(dbmd.othersInsertsAreVisible(type), "Others inserts should be visible for type: " + type);
            }

            // Test types where others inserts are NOT visible
            assertFalse(dbmd.othersInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE),
                    "Others inserts should not be visible for TYPE_SCROLL_SENSITIVE");
            assertFalse(dbmd.othersInsertsAreVisible(1005), // TYPE_SS_SCROLL_KEYSET
                    "Others inserts should not be visible for TYPE_SS_SCROLL_KEYSET");

            // Test detection methods
            for (int type : supportedTypes) {
                assertFalse(dbmd.updatesAreDetected(type), "Updates should not be detected for type: " + type);
                assertFalse(dbmd.insertsAreDetected(type), "Inserts should not be detected for type: " + type);
            }

            // Test deletesAreDetected - only TYPE_SS_SCROLL_KEYSET supports this
            assertFalse(dbmd.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY),
                    "Deletes should not be detected for TYPE_FORWARD_ONLY");
            assertTrue(dbmd.deletesAreDetected(1005), // TYPE_SS_SCROLL_KEYSET
                    "Deletes should be detected for TYPE_SS_SCROLL_KEYSET");

            // Test simple support methods
            assertTrue(dbmd.supportsBatchUpdates(), "Should support batch updates");
            assertTrue(dbmd.supportsGetGeneratedKeys(), "Should support generated keys");
            assertFalse(dbmd.supportsMultipleOpenResults(), "Should not support multiple open results");
            assertTrue(dbmd.supportsNamedParameters(), "Should support named parameters");
            assertTrue(dbmd.supportsSavepoints(), "Should support savepoints");
            assertFalse(dbmd.supportsStatementPooling(), "Should not support statement pooling");
            assertTrue(dbmd.supportsStoredFunctionsUsingCallSyntax(),
                    "Should support stored functions using call syntax");
            assertTrue(dbmd.locatorsUpdateCopy(), "Locators should update copy");

            // Test version methods
            int dbMajorVersion = dbmd.getDatabaseMajorVersion();
            assertTrue(dbMajorVersion >= 0, "Database major version should be non-negative");

            int dbMinorVersion = dbmd.getDatabaseMinorVersion();
            assertTrue(dbMinorVersion >= 0, "Database minor version should be non-negative");

            int jdbcMajorVersion = dbmd.getJDBCMajorVersion();
            assertTrue(jdbcMajorVersion >= 4, "JDBC major version should be at least 4");

            int jdbcMinorVersion = dbmd.getJDBCMinorVersion();
            assertTrue(jdbcMinorVersion >= 0, "JDBC minor version should be non-negative");

            // Test SQL State type
            int sqlStateType = dbmd.getSQLStateType();
            assertTrue(sqlStateType == DatabaseMetaData.sqlStateXOpen || sqlStateType == DatabaseMetaData.sqlStateSQL99,
                    "SQL State type should be either X/Open or SQL99");

            // Test result set holdability
            int holdability = dbmd.getResultSetHoldability();
            assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, holdability,
                    "Default holdability should be HOLD_CURSORS_OVER_COMMIT");

            // Test supportsResultSetHoldability
            assertTrue(dbmd.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT),
                    "Should support HOLD_CURSORS_OVER_COMMIT");
            assertTrue(dbmd.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT),
                    "Should support CLOSE_CURSORS_AT_COMMIT");

            // Test invalid holdability - should throw exception
            assertThrows(SQLException.class, () -> dbmd.supportsResultSetHoldability(9999),
                    "Should throw SQLException for invalid holdability");

            // Test getRowIdLifetime
            RowIdLifetime rowIdLifetime = dbmd.getRowIdLifetime();
            assertEquals(RowIdLifetime.ROWID_UNSUPPORTED, rowIdLifetime, "Row ID lifetime should be ROWID_UNSUPPORTED");

            // Test getConnection
            Connection metaDataConnection = dbmd.getConnection();
            assertNotNull(metaDataConnection, "Connection from metadata should not be null");
            assertSame(conn, metaDataConnection, "Connection should be the same instance");

            // Test getUDTs - should return empty result set
            try (ResultSet rs = dbmd.getUDTs(null, null, null, null)) {
                assertNotNull(rs, "UDTs result set should not be null");
                assertFalse(rs.next(), "UDTs result set should be empty");
            }

            // Test getAttributes - should return empty result set
            try (ResultSet rs = dbmd.getAttributes(null, null, null, null)) {
                assertNotNull(rs, "Attributes result set should not be null");
                assertFalse(rs.next(), "Attributes result set should be empty");
            }

            // Test getSuperTables - should return empty result set
            try (ResultSet rs = dbmd.getSuperTables(null, null, null)) {
                assertNotNull(rs, "SuperTables result set should not be null");
                assertFalse(rs.next(), "SuperTables result set should be empty");
            }

            // Test getSuperTypes - should return empty result set
            try (ResultSet rs = dbmd.getSuperTypes(null, null, null)) {
                assertNotNull(rs, "SuperTypes result set should not be null");
                assertFalse(rs.next(), "SuperTypes result set should be empty");
            }

            // Test invalid result set types for checkResultType (implicitly tested through supportsResultSetType)
            assertThrows(SQLException.class, () -> dbmd.supportsResultSetType(99999),
                    "Should throw SQLException for invalid result set type");

            // Test invalid concurrency types for checkConcurrencyType (implicitly tested through supportsResultSetConcurrency)
            assertThrows(SQLException.class,
                    () -> dbmd.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, 99999),
                    "Should throw SQLException for invalid concurrency type");
        }
    }
}
