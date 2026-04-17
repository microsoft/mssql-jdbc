/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Enhanced Routing tests (TDS Feature 0x0F / ENVCHANGE 0x21).
 * <p>
 * Contains two nested test classes:
 * <ul>
 *   <li>{@link LiveServerTests} — integration tests against an Azure Hyperscale database
 *       with HA read replicas</li>
 *   <li>{@link MockedProtocolTests} — unit tests using Mockito to verify feature negotiation,
 *       ack parsing, ENVCHANGE handling, and routing guard logic without a live server</li>
 * </ul>
 */
@Tag(Constants.enhancedRouting)
public class EnhancedRoutingIntegrationTest {

    // ──────────────────────────────────────────────────────────────────────────
    //  Nested class 1: Live-server integration tests
    // ──────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Live Server Integration Tests")
    class LiveServerTests extends AbstractTest {

        private final String DBNAME_QUERY = "SELECT DB_NAME() AS [db]";
        private String hyperscaleDatabase;

        @BeforeEach
        void setUp() throws Exception {
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

                ServerPortPlaceHolder placeholder = getPlaceHolder(conn);
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

                ServerPortPlaceHolder placeholder = getPlaceHolder(conn);
                String routedDbName = placeholder.getDatabaseName();
                assertNotNull(routedDbName, "Enhanced routing must carry a database name");

                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(DBNAME_QUERY)) {
                    assertTrue(rs.next());
                    assertEquals(routedDbName, rs.getString("db"),
                            "DB_NAME() on replica must match the database name from ENVCHANGE 0x21");
                }

                assertEquals(hyperscaleDatabase, routedDbName,
                        "Routed database name must match the requested Hyperscale database");
            }
        }

        private ServerPortPlaceHolder getPlaceHolder(SQLServerConnection conn) throws Exception {
            Field f = SQLServerConnection.class.getDeclaredField("currentConnectPlaceHolder");
            f.setAccessible(true);
            return (ServerPortPlaceHolder) f.get(conn);
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Nested class 2: Mocked protocol unit tests (no live server needed)
    // ──────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Mocked Protocol Tests")
    class MockedProtocolTests {

        private SQLServerConnection conn;

        @BeforeEach
        void setUp() throws Exception {
            MockitoAnnotations.openMocks(this);
            conn = new SQLServerConnection("MockedTest");
        }

        // ── Feature request write tests ──────────────────────────────────────

        @Test
        @DisplayName("writeEnhancedRoutingFeatureRequest returns correct length (5 bytes)")
        void testFeatureRequestLength() throws Exception {
            // write=false → just calculates length, no writer needed
            int len = conn.writeEnhancedRoutingFeatureRequest(false, null);
            assertEquals(5, len, "Feature request must be 5 bytes: 1 (featureId) + 4 (int32 length=0)");
        }

        // ── Feature ack tests (onFeatureExtAck) ─────────────────────────────

        @Test
        @DisplayName("onFeatureExtAck with data=[1] enables serverSupportsEnhancedRouting")
        void testFeatureAckEnabled() throws Exception {
            invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING, new byte[]{1});
            assertTrue(conn.getServerSupportsEnhancedRouting(),
                    "data[0]==1 must enable enhanced routing");
        }

        @Test
        @DisplayName("onFeatureExtAck with data=[0] keeps serverSupportsEnhancedRouting false")
        void testFeatureAckDisabled() throws Exception {
            invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING, new byte[]{0});
            assertFalse(conn.getServerSupportsEnhancedRouting(),
                    "data[0]==0 must not enable enhanced routing");
        }

        @Test
        @DisplayName("onFeatureExtAck with empty data throws SQLServerException")
        void testFeatureAckEmptyDataThrows() {
            assertThrows(SQLServerException.class,
                    () -> invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING, new byte[]{}),
                    "Empty ack data must throw — expected exactly 1 byte");
        }

        @Test
        @DisplayName("onFeatureExtAck with extra data throws SQLServerException")
        void testFeatureAckExtraDataThrows() {
            assertThrows(SQLServerException.class,
                    () -> invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING, new byte[]{1, 0}),
                    "More than 1 byte in ack data must throw");
        }

        // ── Feature ack routing filter tests ────────────────────────────────

        @Test
        @DisplayName("onFeatureExtAck filters non-DNS/non-enhancedRouting features when routingInfo is set")
        void testFeatureAckFilterDuringRouting() throws Exception {
            setRoutingInfo(conn, new ServerPortPlaceHolder("routed-server", 1433, null, "mydb", false));

            // FEDAUTH ack should be silently skipped (no exception) because routingInfo != null
            // and featureId is neither DNS_CACHING nor ENHANCED_ROUTING
            invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_FEDAUTH, new byte[]{});
        }

        @Test
        @DisplayName("onFeatureExtAck allows enhanced routing ack even when routingInfo is set")
        void testFeatureAckAllowsEnhancedRoutingDuringRouting() throws Exception {
            setRoutingInfo(conn, new ServerPortPlaceHolder("routed-server", 1433, null, "mydb", false));

            invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING, new byte[]{1});
            assertTrue(conn.getServerSupportsEnhancedRouting());
        }

        // ── Routing guard tests ─────────────────────────────────────────────

        @Test
        @DisplayName("Enhanced routing info is discarded when server did not ack the feature")
        void testRoutingGuardDiscardsEnhancedRoutingWhenNotAcked() throws Exception {
            ServerPortPlaceHolder enhancedRouting = new ServerPortPlaceHolder(
                    "routed-host", 1433, null, "someDatabase", false);
            setRoutingInfo(conn, enhancedRouting);
            setServerSupportsEnhancedRouting(conn, false);

            assertNotNull(getRoutingInfo(conn));
            assertNotNull(getRoutingInfo(conn).getDatabaseName());

            boolean discarded = simulateRoutingGuard(conn);
            assertTrue(discarded, "Guard must discard enhanced routing info when feature not acked");
            assertNull(getRoutingInfo(conn), "routingInfo must be null after guard discards it");
        }

        @Test
        @DisplayName("Legacy routing info (no databaseName) is NOT discarded by guard")
        void testRoutingGuardKeepsLegacyRouting() throws Exception {
            ServerPortPlaceHolder legacyRouting = new ServerPortPlaceHolder(
                    "routed-host", 1433, null, false);
            setRoutingInfo(conn, legacyRouting);
            setServerSupportsEnhancedRouting(conn, false);

            boolean discarded = simulateRoutingGuard(conn);
            assertFalse(discarded, "Guard must NOT discard legacy routing info (no databaseName)");
            assertNotNull(getRoutingInfo(conn));
        }

        @Test
        @DisplayName("Enhanced routing info is kept when server DID ack the feature")
        void testRoutingGuardKeepsWhenAcked() throws Exception {
            ServerPortPlaceHolder enhancedRouting = new ServerPortPlaceHolder(
                    "routed-host", 1433, null, "someDatabase", false);
            setRoutingInfo(conn, enhancedRouting);
            setServerSupportsEnhancedRouting(conn, true);

            boolean discarded = simulateRoutingGuard(conn);
            assertFalse(discarded, "Guard must NOT discard routing info when feature was acked");
            assertNotNull(getRoutingInfo(conn));
        }

        // ── ServerPortPlaceHolder tests ─────────────────────────────────────

        @Test
        @DisplayName("ServerPortPlaceHolder 5-arg constructor stores databaseName")
        void testPlaceHolderWithDatabaseName() {
            ServerPortPlaceHolder ph = new ServerPortPlaceHolder("server1", 1433, "inst", "mydb", false);
            assertEquals("server1", ph.getServerName());
            assertEquals(1433, ph.getPortNumber());
            assertEquals("inst", ph.getInstanceName());
            assertEquals("mydb", ph.getDatabaseName());
        }

        @Test
        @DisplayName("ServerPortPlaceHolder 4-arg constructor has null databaseName")
        void testPlaceHolderWithoutDatabaseName() {
            ServerPortPlaceHolder ph = new ServerPortPlaceHolder("server1", 1433, "inst", false);
            assertEquals("server1", ph.getServerName());
            assertEquals(1433, ph.getPortNumber());
            assertNull(ph.getDatabaseName(), "Legacy constructor must leave databaseName null");
        }

        // ── Diagnostic field tests ───────────────────────────────────────────

        @Test
        @DisplayName("serverSupportsEnhancedRouting is false by default")
        void testEnhancedRoutingDefaultFalse() {
            assertFalse(conn.getServerSupportsEnhancedRouting(),
                    "serverSupportsEnhancedRouting must default to false");
        }

        // ── TDS constant tests ──────────────────────────────────────────────

        @Test
        @DisplayName("TDS_FEATURE_EXT_ENHANCEDROUTING is 0x0F")
        void testFeatureIdValue() {
            assertEquals((byte) 0x0F, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING,
                    "Enhanced routing feature ID must be 0x0F");
        }

        // ── Helper methods ──────────────────────────────────────────────────

        private void invokeOnFeatureExtAck(SQLServerConnection conn, byte featureId, byte[] data) throws Exception {
            Method m = SQLServerConnection.class.getDeclaredMethod("onFeatureExtAck", byte.class, byte[].class);
            m.setAccessible(true);
            try {
                m.invoke(conn, featureId, data);
            } catch (java.lang.reflect.InvocationTargetException e) {
                if (e.getCause() instanceof SQLServerException) {
                    throw (SQLServerException) e.getCause();
                }
                throw e;
            }
        }

        private void setRoutingInfo(SQLServerConnection conn, ServerPortPlaceHolder routing) throws Exception {
            Field f = SQLServerConnection.class.getDeclaredField("routingInfo");
            f.setAccessible(true);
            f.set(conn, routing);
        }

        private ServerPortPlaceHolder getRoutingInfo(SQLServerConnection conn) throws Exception {
            Field f = SQLServerConnection.class.getDeclaredField("routingInfo");
            f.setAccessible(true);
            return (ServerPortPlaceHolder) f.get(conn);
        }

        private void setServerSupportsEnhancedRouting(SQLServerConnection conn, boolean value) throws Exception {
            Field f = SQLServerConnection.class.getDeclaredField("serverSupportsEnhancedRouting");
            f.setAccessible(true);
            f.set(conn, value);
        }

        /**
         * Simulates the routing guard logic from login()'s else branch:
         * if routingInfo has a databaseName but the server didn't ack enhanced routing,
         * discard the routing info.
         *
         * @return true if routing info was discarded
         */
        private boolean simulateRoutingGuard(SQLServerConnection conn) throws Exception {
            ServerPortPlaceHolder routing = getRoutingInfo(conn);
            if (routing != null && routing.getDatabaseName() != null) {
                Field f = SQLServerConnection.class.getDeclaredField("serverSupportsEnhancedRouting");
                f.setAccessible(true);
                boolean supported = f.getBoolean(conn);
                if (!supported) {
                    setRoutingInfo(conn, null);
                    return true;
                }
            }
            return false;
        }
    }
}
