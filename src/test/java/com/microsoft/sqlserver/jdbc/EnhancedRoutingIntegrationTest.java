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
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests for Enhanced Routing (TDS Feature 0x0F / ENVCHANGE 0x21).
 */
@Tag(Constants.enhancedRouting)
public class EnhancedRoutingIntegrationTest {

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
        @DisplayName("Feature negotiation: ack disabled [0] sets flag false, ack enabled [1] sets flag true")
        void testFeatureNegotiationEnabledVsDisabled() throws Exception {
            // Fresh connection — default is false
            assertFalse(conn.getServerSupportsEnhancedRouting(),
                    "Default flag value must be false before any ack");

            // Ack disabled
            invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING, new byte[]{0});
            assertFalse(conn.getServerSupportsEnhancedRouting(),
                    "Ack with data[0]=0 must leave flag false");

            // Ack enabled
            invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING, new byte[]{1});
            assertTrue(conn.getServerSupportsEnhancedRouting(),
                    "Ack with data[0]=1 must set flag true");
        }

        @Test
        @DisplayName("onFeatureExtAck filters non-DNS/non-enhancedRouting features when routingInfo is set")
        void testFeatureAckFilterDuringRouting() throws Exception {
            setRoutingInfo(conn, new ServerPortPlaceHolder("routed-server", 1433, null, "mydb", false));

            // FEDAUTH ack should be silently skipped (no exception) because routingInfo != null
            // and featureId is neither DNS_CACHING nor ENHANCED_ROUTING
            invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_FEDAUTH, new byte[]{});
        }

        /**
         * Ack enhanced routing, inject ENVCHANGE 21, call login() via reflection.
         * Verifies login() guard accepts routing and sets currentConnectPlaceHolder.
         */
        @Test
        @DisplayName("Enhanced routing acked — login() accepts routing to different server and database")
        void testRoutedConnectionViaLogin() throws Exception {
            String originalDatabase = "OriginalDB";
            String routedDatabase = "RoutedReplicaDB";
            String routedServer = "routed-replica.database.windows.net";
            int routedPort = 1433;

            // Client initially connected to OriginalDB
            setupActiveConnectionProperties(conn);
            Properties props = getActiveConnectionProperties(conn);
            props.setProperty(SQLServerDriverStringProperty.DATABASE_NAME.toString(), originalDatabase);

            // Server acks enhanced routing during feature negotiation
            invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING, new byte[]{1});
            assertTrue(conn.getServerSupportsEnhancedRouting(), "Feature must be acked");

            // Server sends ENVCHANGE 21 routing client to a different database
            TDSReader tdsReader = createTdsReaderWithEnvChange(conn, 21, routedServer, routedPort, routedDatabase);
            conn.processEnvChange(tdsReader);

            // Invoke actual login() — the routing guard runs in production code
            ServerPortPlaceHolder routingTarget = invokeLoginAndGetRoutingTarget(conn);

            // login() accepted the routing — currentConnectPlaceHolder has the routed target
            assertNotNull(routingTarget,
                    "login() guard must accept routingInfo when server acked enhanced routing");
            assertEquals(routedServer, routingTarget.getServerName(),
                    "Driver must reconnect to the routed server");
            assertEquals(routedPort, routingTarget.getPortNumber(),
                    "Driver must reconnect to the routed port");
            assertEquals(routedDatabase, routingTarget.getDatabaseName(),
                    "Login7 must use routed database, not original");

            // routingInfo consumed by login()
            assertNull(getRoutingInfo(conn),
                    "routingInfo must be consumed after login() guard accepts it");
        }

        @Test
        @DisplayName("Legacy ENVCHANGE 20 → routing info carries server and port but no database name")
        void testFullFlowLegacyRoutingNoDatabaseName() throws Exception {
            setupActiveConnectionProperties(conn);

            TDSReader tdsReader = createTdsReaderWithEnvChange(conn, 20,
                    "routed-server.database.windows.net", 1433, null);
            conn.processEnvChange(tdsReader);

            ServerPortPlaceHolder routing = getRoutingInfo(conn);
            assertNotNull(routing, "processEnvChange must set routingInfo for legacy routing");
            assertEquals("routed-server.database.windows.net", routing.getServerName());
            assertEquals(1433, routing.getPortNumber());
            assertNull(routing.getDatabaseName(), "Legacy ENVCHANGE 20 must not carry a database name");
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

        /** No feature ack — login() guard discards routingInfo. */
        @Test
        @DisplayName("No feature ack — login guard discards routing, original DB preserved")
        void testServerDoesNotRouteNoAck() throws Exception {
            setupActiveConnectionProperties(conn);
            Properties props = getActiveConnectionProperties(conn);
            props.setProperty(SQLServerDriverStringProperty.DATABASE_NAME.toString(), "master");

            // No feature ack — flag stays false (default)
            assertFalse(conn.getServerSupportsEnhancedRouting());

            // Server sends ENVCHANGE 21 with routed DB
            TDSReader tdsReader = createTdsReaderWithEnvChange(conn, 21,
                    "routed.database.windows.net", 1433, "RoutedDB");
            conn.processEnvChange(tdsReader);

            // Invoke actual login() via reflection — the routing guard runs in production code
            ServerPortPlaceHolder routingTarget = invokeLoginAndGetRoutingTarget(conn);

            // Guard discarded the routing — connection stays on original server
            assertNull(routingTarget,
                    "login() guard must discard routingInfo when server did not ack enhanced routing");
            assertNull(getRoutingInfo(conn),
                    "routingInfo field must be null after login() guard discards it");
        }

        /** Feature ack data[0]=0 (disabled) — login() guard discards routingInfo. */
        @Test
        @DisplayName("Feature ack disabled — login guard discards routing, original DB preserved")
        void testServerDoesNotRouteDisabled() throws Exception {
            setupActiveConnectionProperties(conn);
            Properties props = getActiveConnectionProperties(conn);
            props.setProperty(SQLServerDriverStringProperty.DATABASE_NAME.toString(), "master");

            // Server acks with data[0]=0 — explicitly disabled
            invokeOnFeatureExtAck(conn, TDS.TDS_FEATURE_EXT_ENHANCEDROUTING, new byte[]{0});
            assertFalse(conn.getServerSupportsEnhancedRouting());

            // Server sends ENVCHANGE 21
            TDSReader tdsReader = createTdsReaderWithEnvChange(conn, 21,
                    "routed.database.windows.net", 1433, "RoutedDB");
            conn.processEnvChange(tdsReader);

            // Invoke actual login() via reflection — the routing guard runs in production code
            ServerPortPlaceHolder routingTarget = invokeLoginAndGetRoutingTarget(conn);

            assertNull(routingTarget,
                    "login() guard must discard routingInfo when server disabled enhanced routing");
            assertNull(getRoutingInfo(conn),
                    "routingInfo field must be null after login() guard discards it");
        }

        // Helper methods

        /**
         * Calls login() via reflection. Reads {@code currentConnectPlaceHolder} afterward:
         * non-null means routing accepted, null means discarded.
         */
        private ServerPortPlaceHolder invokeLoginAndGetRoutingTarget(SQLServerConnection conn) throws Exception {
            // Reset the field so we can detect whether login() set it
            Field connectField = SQLServerConnection.class.getDeclaredField("currentConnectPlaceHolder");
            connectField.setAccessible(true);
            connectField.set(conn, null);

            Method loginMethod = SQLServerConnection.class.getDeclaredMethod("login",
                    String.class, String.class, int.class, String.class,
                    FailoverInfo.class, int.class, long.class);
            loginMethod.setAccessible(true);

            try {
                loginMethod.invoke(conn, "localhost", null, 1433, null, null, 30, System.currentTimeMillis());
            } catch (java.lang.reflect.InvocationTargetException e) {
                // Both paths crash eventually — that's expected
            }

            // Read the field: non-null means routing was accepted, null means discarded
            return (ServerPortPlaceHolder) connectField.get(conn);
        }

        private Properties getActiveConnectionProperties(SQLServerConnection conn) throws Exception {
            Field f = SQLServerConnection.class.getDeclaredField("activeConnectionProperties");
            f.setAccessible(true);
            return (Properties) f.get(conn);
        }

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

        /** Initializes required fields on conn to avoid NPEs in processEnvChange/TDSReader. */
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

        /** Crafts a TDS ENVCHANGE routing token (type 20 or 21) and returns a positioned TDSReader. */
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
