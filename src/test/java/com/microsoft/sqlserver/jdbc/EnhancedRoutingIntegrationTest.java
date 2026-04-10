/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Enhanced Routing integration tests (TDS Feature 0x0F / ENVCHANGE 0x21).
 * Validates connectivity, feature negotiation, and database name routing
 * against an Azure Hyperscale database with HA read replicas.
 */
@Tag(Constants.enhancedRouting)
public class EnhancedRoutingIntegrationTest extends AbstractTest {

    private static final String DBNAME_QUERY = "SELECT DB_NAME() AS [db]";

    private static String hyperscaleDatabase;

    @BeforeAll
    static void setupHyperscale() throws Exception {
        setConnection();
        hyperscaleDatabase = getConfiguredProperty("hyperscaleDatabase", null);
        assumeTrue(hyperscaleDatabase != null && !hyperscaleDatabase.isEmpty(),
                "Skipping: 'hyperscaleDatabase' not configured.");
    }

    private String buildConnectionString(String applicationIntent) {
        String connStr = connectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, "database", hyperscaleDatabase);
        connStr = TestUtils.addOrOverrideProperty(connStr, "encrypt", "true");
        connStr = TestUtils.addOrOverrideProperty(connStr, "trustServerCertificate", "false");
        connStr = TestUtils.addOrOverrideProperty(connStr, "loginTimeout", "30");
        if (applicationIntent != null) {
            connStr = TestUtils.addOrOverrideProperty(connStr, "applicationIntent", applicationIntent);
        }
        return connStr;
    }

    @Test
    @DisplayName("Connect to Hyperscale database")
    void testConnectivity() throws SQLException {
        try (Connection conn = DriverManager.getConnection(buildConnectionString("ReadOnly"));
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 1 AS [ok]")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("ok"));
        }
    }

    @Test
    @DisplayName("Server supports enhanced routing (ENVCHANGE 0x21)")
    void testServerSupportsEnhancedRouting() throws Exception {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager
                .getConnection(buildConnectionString("ReadOnly"))) {

            assertTrue(conn.getServerSupportsEnhancedRouting(),
                    "Server must acknowledge enhanced routing feature (TDS_FEATURE_EXT 0x0F)");

            Field f = SQLServerConnection.class.getDeclaredField("currentConnectPlaceHolder");
            f.setAccessible(true);
            ServerPortPlaceHolder placeholder = (ServerPortPlaceHolder) f.get(conn);
            assertNotNull(placeholder, "Connection must have been routed");
            assertNotNull(placeholder.getDatabaseName(),
                    "ENVCHANGE 0x21 must carry a database name — confirms enhanced routing (type 21), "
                            + "not legacy routing (type 20)");
        }
    }

    @Test
    @DisplayName("Routed database name matches DB_NAME() on replica")
    void testRoutedDatabaseNameMatchesActual() throws Exception {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager
                .getConnection(buildConnectionString("ReadOnly"))) {

            Field f = SQLServerConnection.class.getDeclaredField("currentConnectPlaceHolder");
            f.setAccessible(true);
            ServerPortPlaceHolder placeholder = (ServerPortPlaceHolder) f.get(conn);
            String routedDbName = placeholder.getDatabaseName();
            assertNotNull(routedDbName, "Enhanced routing must carry a database name");

            // The database name the server sent via ENVCHANGE 0x21 must match
            // what the replica actually reports as DB_NAME()
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(DBNAME_QUERY)) {
                assertTrue(rs.next());
                assertEquals(routedDbName, rs.getString("db"),
                        "DB_NAME() on replica must match the database name from ENVCHANGE 0x21");
            }

            // And both must match the originally requested database
            assertEquals(hyperscaleDatabase, routedDbName,
                    "Routed database name must match the requested Hyperscale database");
        }
    }
}
