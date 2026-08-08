/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Standalone manual test / mock example that connects to a SQL Server which processes the Client OpenTelemetry TDS
 * feature extension (feature id {@code 0x11}) and reports whether the feature was successfully negotiated.
 *
 * <p>This class lives in the {@code com.microsoft.sqlserver.jdbc} package so it can read the package-private
 * negotiation getters ({@link SQLServerConnection#isClientOpenTelemetryEnabled()},
 * {@link SQLServerConnection#getOtelServerRegion()}, {@link SQLServerConnection#getOtelServerArmResourceId()},
 * {@link SQLServerConnection#getResolvedOpenTelemetryConfig()}) directly off the live connection.</p>
 *
 * <p>The connection string is read from the {@code onebox_connection_string} environment variable. Run it with:</p>
 *
 * <pre>
 * mvn -Pjre17 -pl mssql-jdbc-core -DskipITs -Dtest=ClientOpenTelemetryNegotiationSample#run test
 * </pre>
 *
 * <p>or as a plain {@code main} once the test classes are on the classpath.</p>
 */
public final class ClientOpenTelemetryNegotiationSample {

    private ClientOpenTelemetryNegotiationSample() {}

    /** Environment variable holding the target connection string. */
    static final String ENV_CONNECTION_STRING = "onebox_connection_string";

    public static void main(String[] args) {
        enableFinerLogging();

        String connectionString = System.getenv(ENV_CONNECTION_STRING);
        if (null == connectionString || connectionString.isEmpty()) {
            System.err.println("ERROR: environment variable '" + ENV_CONNECTION_STRING + "' is not set.");
            System.exit(2);
            return;
        }

        // Ensure the driver is registered even when running from a bare classpath without the
        // META-INF/services/java.sql.Driver service descriptor.
        try {
            DriverManager.registerDriver(new SQLServerDriver());
        } catch (Exception e) {
            System.err.println("ERROR: could not register SQLServerDriver: " + e);
            System.exit(4);
            return;
        }

        System.out.println("Connecting to server to negotiate the Client OpenTelemetry feature extension (0x11)...");

        try (Connection connection = DriverManager.getConnection(connectionString)) {
            if (!connection.isWrapperFor(SQLServerConnection.class)) {
                System.err.println("ERROR: connection is not a SQLServerConnection (got "
                        + connection.getClass().getName() + ").");
                System.exit(3);
                return;
            }

            SQLServerConnection sqlConnection = connection.unwrap(SQLServerConnection.class);

            boolean enabled = sqlConnection.isClientOpenTelemetryEnabled();
            String region = sqlConnection.getOtelServerRegion();
            String armResourceId = sqlConnection.getOtelServerArmResourceId();
            OpenTelemetryConfig resolved = sqlConnection.getResolvedOpenTelemetryConfig();

            System.out.println();
            System.out.println("========== Client OpenTelemetry negotiation result ==========");
            System.out.println("  feature acknowledged / enabled : " + enabled);
            System.out.println("  server region                  : [" + region + "]"
                    + (region.isEmpty() ? " (empty -> box / Arc)" : ""));
            System.out.println("  server ARM resource id         : [" + armResourceId + "]"
                    + (armResourceId.isEmpty() ? " (empty -> box / Arc)" : ""));
            System.out.println("  resolved telemetry mode        : " + resolved.getMode());
            System.out.println("  resolved endpoint              : " + resolved.getEndpoint());
            System.out.println("  resolved auth header names     : " + resolved.getAuthHeaders().keySet());
            System.out.println("=============================================================");
            System.out.println();

            if (enabled) {
                System.out.println("SUCCESS: server processed the Client OpenTelemetry feature extension and the "
                        + "acknowledgement was received and parsed.");
            } else {
                System.out.println("NOTE: the feature ack was received but the server reported the feature as "
                        + "DISABLED (bEnabled = 0), or the server did not include an ack for feature 0x11.");
            }
        } catch (Exception e) {
            System.err.println("ERROR: failed to connect or negotiate: " + e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    /** Turns on FINER console logging for the connection logger so the raw ack bytes and parsed values are shown. */
    private static void enableFinerLogging() {
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.FINER);

        Logger connectionLogger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerConnection");
        connectionLogger.setLevel(Level.FINER);
        connectionLogger.addHandler(handler);
        connectionLogger.setUseParentHandlers(false);
    }
}
