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
import java.sql.SQLException;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerException;
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
     * //TODO: Test for capability 1.7 for JDK 1.7 and 1.8 for 1.8 //Require-Capability: osgi.ee;filter:="(&(osgi.ee=JavaSE)(version=1.8))" 
     * //String capability = attributes.getValue("Require-Capability");
     * 
     * @throws SQLServerException
     *             Our Wrapped Exception
     * @throws SQLException
     *             SQL Exception
     * @throws IOException
     *             IOExcption
     */
    @Test
    public void testDriverVersion() throws SQLServerException, SQLException, IOException {
        String manifestFile = Utils.getCurrentClassPath() + "META-INF/MANIFEST.MF";
        manifestFile = manifestFile.replace("test-classes", "classes");

        File f = new File(manifestFile);

        assumeTrue(f.exists(), "Manifest file is not exist on classpath so ignoring test");

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
            assertTrue(intDriverVersion < intBuildVersion, "In case of SNAPSHOT version build version should be always greater than BuildVersion");
        }
        else {
            assertTrue(intDriverVersion == intBuildVersion, "For NON SNAPSHOT versions build & driver versions should match.");
        }

    }

    /**
     * Your password should not be in getURL method. 
     * 
     * @throws SQLServerException 
     * @throws SQLException
     */
    @Test
    public void testGetURL() throws SQLServerException, SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        String url = databaseMetaData.getURL();
        url = url.toLowerCase();
        assertFalse(url.contains("password"), "Get URL should not have password attribute / property."); 
    }

    /**
     * Test getUsername.
     * 
     * @throws SQLServerException
     * @throws SQLException
     */
    @Test
    public void testDBUserLogin() throws SQLServerException, SQLException {
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

}
