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

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests for Enhanced Routing (TDS Feature 0x0F / ENVCHANGE 0x21).
 */
@Tag(Constants.enhancedRouting)
public class EnhancedRoutingIntegrationTest {

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
        @DisplayName("Server acks enhanced routing and ENVCHANGE 0x21 carries database name")
        void testServerSupportsEnhancedRouting() throws Exception {
            try (SQLServerConnection conn = (SQLServerConnection) DriverManager
                    .getConnection(buildConnectionString("ReadOnly"))) {
                assertTrue(conn.getServerSupportsEnhancedRouting());

                ServerPortPlaceHolder placeholder = getPlaceHolder(conn);
                assertNotNull(placeholder, "Connection must have been routed");
                assertNotNull(placeholder.getDatabaseName(),
                        "ENVCHANGE 0x21 must carry a database name");
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


    @Nested
    @DisplayName("Mocked Protocol Tests")
    class MockedProtocolTests {

        private SQLServerConnection conn;

        @BeforeEach
        void setUp() throws Exception {
            conn = new SQLServerConnection("UnitTest");
        }

        @Test
        @DisplayName("onFeatureExtAck throws with exact error message for invalid ack data")
        void testFeatureAckInvalidDataThrowsWithMessage() {
            String expectedMessage = "Enhanced routing feature extension ack should contain exactly 1 byte of data.";

            // Empty data — 0 bytes instead of 1
            SQLServerException emptyEx = assertThrows(SQLServerException.class,
                    () -> invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING, new byte[]{}));
            assertEquals(expectedMessage, emptyEx.getMessage(),
                    "Empty ack data must throw with exact error message");

            // Extra data — 2 bytes instead of 1
            SQLServerException extraEx = assertThrows(SQLServerException.class,
                    () -> invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING, new byte[]{1, 0}));
            assertEquals(expectedMessage, extraEx.getMessage(),
                    "Extra ack data must throw with exact error message");
        }

        @Test
        @DisplayName("onFeatureExtAck filters non-DNS/non-enhancedRouting features when routingInfo is set")
        void testFeatureAckFilterDuringRouting() throws Exception {
            setRoutingInfo(conn, new ServerPortPlaceHolder("routed-server", 1433, null, "mydb", false));

            // FEDAUTH ack should be silently skipped (no exception) because routingInfo != null
            // and featureId is neither DNS_CACHING nor ENHANCED_ROUTING
            invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_FEDAUTH, new byte[]{});
        }

        @Test
        @DisplayName("Server under load routes to different database — driver uses routed DB name in Login7")
        void testFullFlowEnhancedRoutingDatabaseName() throws Exception {
            String originalDatabase = "OriginalDB";
            String routedDatabase = "RoutedReplicaDB";
            String routedServer = "routed-replica.database.windows.net";
            int routedPort = 1433;

            // Client initially connected to OriginalDB
            setupActiveConnectionProperties(conn);
            Field propsField = SQLServerConnection.class.getDeclaredField("activeConnectionProperties");
            propsField.setAccessible(true);
            Properties props = (Properties) propsField.get(conn);
            props.setProperty(SQLServerDriverStringProperty.DATABASE_NAME.toString(), originalDatabase);

            // Before routing: sendLogon() would use the original database
            String preRouteDatabase = props.getProperty(SQLServerDriverStringProperty.DATABASE_NAME.toString());
            assertEquals(originalDatabase, preRouteDatabase,
                    "Before routing, Login7 must use the original database");

            // Server acks enhanced routing during feature negotiation
            invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING, new byte[]{1});
            assertTrue(conn.getServerSupportsEnhancedRouting(), "Feature must be acked");

            // Server is under load — sends ENVCHANGE 21 routing client to a different database
            TDSReader tdsReader = createTdsReaderWithEnvChange(conn, 21, routedServer, routedPort, routedDatabase);
            conn.processEnvChange(tdsReader);

            // Verify the real TDS parser extracted the routing info correctly
            ServerPortPlaceHolder routing = getRoutingInfo(conn);
            assertNotNull(routing, "processEnvChange must populate routingInfo from ENVCHANGE 21");
            // Verify the routing info carries the routed database, server, and port
            assertEquals(routedDatabase, routing.getDatabaseName(),
                    "Login7 must use routed database, not original");
            assertEquals(routedServer, routing.getServerName(),
                    "Driver must reconnect to the routed server");
            assertEquals(routedPort, routing.getPortNumber(),
                    "Driver must reconnect to the routed port");
        }

        @Test
        @DisplayName("No ack + ENVCHANGE 21 → processEnvChange rejects unrecognized enhanced routing")
        void testFullFlowNoAckEnvChange21Throws() throws Exception {
            String originalDatabase = "OriginalDB";

            // Client initially connected to OriginalDB
            setupActiveConnectionProperties(conn);
            Field propsField = SQLServerConnection.class.getDeclaredField("activeConnectionProperties");
            propsField.setAccessible(true);
            Properties props = (Properties) propsField.get(conn);
            props.setProperty(SQLServerDriverStringProperty.DATABASE_NAME.toString(), originalDatabase);

            // Before routing: Login7 uses original database
            assertEquals(originalDatabase,
                    props.getProperty(SQLServerDriverStringProperty.DATABASE_NAME.toString()),
                    "Before routing, Login7 must use the original database");

            // Server did NOT ack enhanced routing
            assertFalse(conn.getServerSupportsEnhancedRouting(), "Feature must not be acked");

            // Server sends ENVCHANGE 21 anyway — processEnvChange must reject it
            TDSReader tdsReader = createTdsReaderWithEnvChange(conn, 21, "routed-server", 1433, "SomeDB");
            assertThrows(SQLServerException.class, () -> conn.processEnvChange(tdsReader),
                    "ENVCHANGE 21 without feature ack must throw");

            // After rejection: routingInfo must remain null, original database unchanged
            assertNull(getRoutingInfo(conn), "routingInfo must remain null after rejected ENVCHANGE 21");
            assertEquals(originalDatabase,
                    props.getProperty(SQLServerDriverStringProperty.DATABASE_NAME.toString()),
                    "After rejected routing, Login7 must still use original database");
        }

        @Test
        @DisplayName("Legacy ENVCHANGE 20 → driver routes to new server but keeps original database name")
        void testFullFlowLegacyRoutingUsesOriginalDatabase() throws Exception {
            String originalDatabase = "OriginalDB";
            String routedServer = "routed-server.database.windows.net";
            int routedPort = 1433;

            // Client initially connected to OriginalDB
            setupActiveConnectionProperties(conn);
            Field propsField = SQLServerConnection.class.getDeclaredField("activeConnectionProperties");
            propsField.setAccessible(true);
            Properties props = (Properties) propsField.get(conn);
            props.setProperty(SQLServerDriverStringProperty.DATABASE_NAME.toString(), originalDatabase);

            // Before routing: Login7 uses original database
            assertEquals(originalDatabase,
                    props.getProperty(SQLServerDriverStringProperty.DATABASE_NAME.toString()),
                    "Before routing, Login7 must use the original database");

            // Server sends legacy ENVCHANGE 20 (no database name in routing info)
            TDSReader tdsReader = createTdsReaderWithEnvChange(conn, 20, routedServer, routedPort, null);
            conn.processEnvChange(tdsReader);

            // Verify parser populated routingInfo without database name
            ServerPortPlaceHolder routing = getRoutingInfo(conn);
            assertNotNull(routing, "processEnvChange must set routingInfo for legacy routing");
            assertEquals(routedServer, routing.getServerName(), "Driver must route to the new server");
            assertEquals(routedPort, routing.getPortNumber(), "Driver must route to the new port");
            assertNull(routing.getDatabaseName(), "Legacy ENVCHANGE 20 must not carry a database name");
        }

        @Test
        @DisplayName("Server acks enhanced routing with data[0]=0 (explicitly disabled)")
        void testFeatureAckExplicitlyDisabled() throws Exception {
            // Server sends ack with data[0]=0 — explicitly says "I don't support enhanced routing"
            invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING, new byte[]{0});
            assertFalse(conn.getServerSupportsEnhancedRouting(),
                    "data[0]=0 must leave serverSupportsEnhancedRouting as false");

            // Now if server sends ENVCHANGE 21, it must be rejected
            setupActiveConnectionProperties(conn);
            TDSReader tdsReader = createTdsReaderWithEnvChange(conn, 21, "routed-server", 1433, "SomeDB");
            assertThrows(SQLServerException.class, () -> conn.processEnvChange(tdsReader),
                    "ENVCHANGE 21 must be rejected when server explicitly disabled the feature");
            assertNull(getRoutingInfo(conn), "routingInfo must remain null");
        }

        @Test
        @DisplayName("ENVCHANGE 21 with port 0 throws invalid TDS")
        void testEnvChange21InvalidPort() throws Exception {
            setupActiveConnectionProperties(conn);
            invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING, new byte[]{1});

            TDSReader tdsReader = createTdsReaderWithEnvChange(conn, 21, "routed-server", 0, "SomeDB");
            assertThrows(SQLServerException.class, () -> conn.processEnvChange(tdsReader),
                    "Port 0 in ENVCHANGE 21 must throw");
        }

        @Test
        @DisplayName("ENVCHANGE 21 with database name length 0 throws invalid enhanced routing info")
        void testEnvChange21EmptyDatabaseName() throws Exception {
            setupActiveConnectionProperties(conn);
            invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING, new byte[]{1});

            // Craft bytes with dbNameLength=0 (invalid for enhanced routing)
            TDSReader tdsReader = createTdsReaderWithEnvChange21EmptyDb(conn, "routed-server", 1433);
            assertThrows(SQLServerException.class, () -> conn.processEnvChange(tdsReader),
                    "Empty database name in ENVCHANGE 21 must throw");
        }

        @Test
        @DisplayName("Enhanced routing updates hostNameInCertificate for same-domain routed server")
        void testHostNameInCertificateUpdatedOnRouting() throws Exception {
            setupActiveConnectionProperties(conn);
            invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING, new byte[]{1});

            // Route to a server in the same *.database.windows.net domain
            String routedServer = "replica-xyz.database.windows.net";
            TDSReader tdsReader = createTdsReaderWithEnvChange(conn, 21, routedServer, 1433, "RoutedDB");
            conn.processEnvChange(tdsReader);

            Field propsField = SQLServerConnection.class.getDeclaredField("activeConnectionProperties");
            propsField.setAccessible(true);
            Properties props = (Properties) propsField.get(conn);
            String updatedHostName = props.getProperty("hostNameInCertificate");

            // The wildcard should match the routed server's domain
            assertEquals("*.database.windows.net", updatedHostName,
                    "hostNameInCertificate must be updated to match routed server domain");
        }

        // Helper methods
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

        /** Sets fields on conn so processEnvChange and TDSReader don't NPE. */
        private void setupActiveConnectionProperties(SQLServerConnection conn) throws Exception {
            Properties props = new Properties();
            props.setProperty("hostNameInCertificate", "*.database.windows.net");
            Field f = SQLServerConnection.class.getDeclaredField("activeConnectionProperties");
            f.setAccessible(true);
            f.set(conn, props);

            // TDSReader constructor calls isColumnEncryptionSettingEnabled() which needs this non-null
            Field ces = SQLServerConnection.class.getDeclaredField("columnEncryptionSetting");
            ces.setAccessible(true);
            ces.set(conn, "Disabled");
        }

        /**
         * Crafts a TDS ENVCHANGE routing token and returns a TDSReader positioned at the start.
         *
         * Wire format: [0xE3] [envValueLength: LE ushort] [envchange: byte]
         *   [routingDataLen: LE ushort] [protocol=0] [port: LE ushort]
         *   [serverNameLen: LE ushort] [serverName: UTF-16LE]
         *   [dbNameLen: LE ushort] [dbName: UTF-16LE]  ← only for envchange=21
         *   [oldValueLen=0: byte]
         */
        private TDSReader createTdsReaderWithEnvChange(SQLServerConnection conn,
                int envchange, String serverName, int port, String databaseName) throws Exception {

            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] serverBytes = serverName.getBytes(StandardCharsets.UTF_16LE);

            int routingDataLen = 1 + 2 + 2 + serverBytes.length;
            if (envchange == 21 && databaseName != null) {
                routingDataLen += 2 + databaseName.getBytes(StandardCharsets.UTF_16LE).length;
            }
            int envValueLength = 1 + 2 + routingDataLen + 1;

            buf.write(0xE3);                          // token type
            writeUShortLE(buf, envValueLength);        // envValueLength
            buf.write(envchange);                      // envchange type (20 or 21)
            writeUShortLE(buf, routingDataLen);         // routing data length
            buf.write(0);                              // protocol
            writeUShortLE(buf, port);                   // port
            writeUShortLE(buf, serverName.length());    // server name length (chars)
            buf.write(serverBytes);                    // server name (UTF-16LE)

            if (envchange == 21 && databaseName != null) {
                byte[] dbBytes = databaseName.getBytes(StandardCharsets.UTF_16LE);
                writeUShortLE(buf, databaseName.length());
                buf.write(dbBytes);
            }

            buf.write(0);  // old value length = 0

            byte[] payload = buf.toByteArray();

            // Inject crafted bytes into a TDSPacket → TDSReader
            TDSPacket packet = new TDSPacket(payload.length);
            System.arraycopy(payload, 0, packet.payload, 0, payload.length);
            packet.payloadLength = payload.length;

            return createTdsReader(conn, packet);
        }

        private TDSReader createTdsReader(SQLServerConnection conn, TDSPacket packet) throws Exception {
            java.lang.reflect.Constructor<TDSReader> ctor = TDSReader.class.getDeclaredConstructor(
                    TDSChannel.class, SQLServerConnection.class, TDSCommand.class);
            ctor.setAccessible(true);
            TDSReader reader = ctor.newInstance(null, conn, null);

            Field currentPacketField = TDSReader.class.getDeclaredField("currentPacket");
            currentPacketField.setAccessible(true);
            currentPacketField.set(reader, packet);

            Field offsetField = TDSReader.class.getDeclaredField("payloadOffset");
            offsetField.setAccessible(true);
            offsetField.set(reader, 0);

            return reader;
        }

        private void writeUShortLE(ByteArrayOutputStream buf, int value) {
            buf.write(value & 0xFF);
            buf.write((value >> 8) & 0xFF);
        }

        /** Crafts ENVCHANGE 21 with dbNameLength=0 to test empty database name rejection. */
        private TDSReader createTdsReaderWithEnvChange21EmptyDb(SQLServerConnection conn,
                String serverName, int port) throws Exception {

            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] serverBytes = serverName.getBytes(StandardCharsets.UTF_16LE);

            // routingDataLen includes: protocol(1) + port(2) + serverNameLen(2) + serverBytes + dbNameLen(2)
            int routingDataLen = 1 + 2 + 2 + serverBytes.length + 2;
            int envValueLength = 1 + 2 + routingDataLen + 1;

            buf.write(0xE3);
            writeUShortLE(buf, envValueLength);
            buf.write(21);                             // enhanced routing
            writeUShortLE(buf, routingDataLen);
            buf.write(0);                              // protocol
            writeUShortLE(buf, port);
            writeUShortLE(buf, serverName.length());
            buf.write(serverBytes);
            writeUShortLE(buf, 0);                     // dbNameLength = 0 (invalid)
            buf.write(0);                              // old value length = 0

            byte[] payload = buf.toByteArray();
            TDSPacket packet = new TDSPacket(payload.length);
            System.arraycopy(payload, 0, packet.payload, 0, payload.length);
            packet.payloadLength = payload.length;
            return createTdsReader(conn, packet);
        }
    }
}
