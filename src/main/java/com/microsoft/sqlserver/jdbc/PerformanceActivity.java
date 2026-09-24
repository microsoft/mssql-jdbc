/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Enum representing different performance activities tracked by the driver.
 * 
 * Connection-Level Activities:
 * - CONNECTION: Total connection establishment time
 * - PRELOGIN: TDS prelogin negotiation time
 * - LOGIN: Authentication/login handshake time
 * - TOKEN_ACQUISITION: Azure AD token acquisition time
 * 
 * Statement-Level Activities:
 * - STATEMENT_REQUEST_BUILD: Client-side request building time
 * - STATEMENT_FIRST_SERVER_RESPONSE: Time from packet sent to first response received
 * - STATEMENT_PREPARE: sp_prepare execution time
 * - STATEMENT_PREPEXEC: sp_prepexec (combined prepare+execute) time
 * - STATEMENT_EXECUTE: Statement execution time
 */
public enum PerformanceActivity {

    // ==================== Connection-Level Activities ====================

    /**
     * Total time to establish a connection.
     * 
     * Measured in: connectInternal()
     * Includes: PRELOGIN, LOGIN, and TOKEN_ACQUISITION (if applicable)
     */
    CONNECTION("Connection"),

    /**
     * Time for TDS prelogin negotiation.
     * 
     * Measured in: prelogin()
     * Includes: Encryption negotiation, server capability detection
     */
    PRELOGIN("Prelogin"),

    /**
     * Time for authentication/login handshake.
     * 
     * Measured in: login()
     * Includes: TDS login7 packet exchange
     */
    LOGIN("Login"),

    /**
     * Time to acquire Azure AD access tokens.
     * 
     * Measured in: onFedAuthInfo()
     * Triggered: Only for Active Directory authentication methods
     */
    TOKEN_ACQUISITION("Token acquisition"),

    // ==================== Statement-Level Activities ====================

    /**
     * Client-side request building time.
     * 
     * Measures: Time from execute*() call until TDS request packet is ready to send.
     * 
     * Start: Beginning of executeStatement() (inside retry loop)
     * End: Just before startResponse() sends packet to server
     * 
     * Note: Restarts on retry to exclude backoff/sleep time.
     */
    STATEMENT_REQUEST_BUILD("Request build time"),

    /**
     * Time from first packet sent to first response received.
     * 
     * Measures: Network latency (both directions) + SQL Server processing time.
     * 
     * Start: startResponse() begins - packet sent to server
     * End: First response packet received from server
     * 
     * In non-adaptive (full buffering) mode this therefore reflects the time to
     * fully buffer the entire response on the client, not just the first-byte latency.
     * 
     * This represents the "server-side" time from client's perspective,
     * including network transit in both directions.
     */
    STATEMENT_FIRST_SERVER_RESPONSE("First server response"),

    /**
     * Time to prepare a statement using sp_prepare.
     * 
     * Measured in: doPrep()
     * Triggered: Only when prepareMethod=prepare (NOT the default)
     */
    STATEMENT_PREPARE("Statement prepare"),

    /**
     * Combined prepare+execute time using sp_prepexec.
     * 
     * This is the DEFAULT prepare method. SQL Server combines preparation
     * and execution in a single sp_prepexec stored procedure call.
     */
    STATEMENT_PREPEXEC("Statement prepexec"),

    /**
     * Statement execution time.
     * 
     * Measures: Time from sending execution request to response processing complete.
     * 
     * For Statement: Full execution time of SQL query.
     */
    STATEMENT_EXECUTE("Statement execute"),

    // Lifecycle-only activities. Appended to preserve existing ordinals; never passed to legacy publish callbacks.
    /** Validated connection configuration. */
    CONNECTION_CONFIGURATION("Connection configuration"),
    /** One endpoint connection attempt, not a parallel socket candidate. */
    CONNECTION_ATTEMPT("Connection attempt"),
    /** SQL Browser instance discovery. */
    INSTANCE_DISCOVERY("Instance discovery"),
    /** An actual host resolution operation. */
    DNS("DNS"),
    /** Socket connection establishment. */
    SOCKET_CONNECT("Socket connect"),
    /** TLS negotiation. */
    TLS("TLS"),
    /** Narrow TDS login exchange, unlike the legacy LOGIN wrapper. */
    LOGIN_EXCHANGE("Login exchange"),
    /** Actual token request, unlike the legacy TOKEN_ACQUISITION wrapper. */
    TOKEN_REQUEST("Token request"),
    /** Required post-login connection initialization. */
    CONNECTION_INITIALIZE("Connection initialize"),
    /** Processing a server routing transition after the login exchange. */
    CONNECTION_REDIRECT("Connection redirect");

    private final String activity;

    PerformanceActivity(String activity) {
        this.activity = activity;
    }

    public String activity() {
        return activity;
    }

    // Deliberately not inferred from display names: old broad wrappers have no lifecycle phase.
    String connectionPhase() {
        switch (this) {
            case CONNECTION:
                return "connection.open";
            case PRELOGIN:
                return "prelogin";
            case CONNECTION_CONFIGURATION:
                return "configuration";
            case CONNECTION_ATTEMPT:
                return "attempt";
            case INSTANCE_DISCOVERY:
                return "instance_discovery";
            case DNS:
                return "dns";
            case SOCKET_CONNECT:
                return "socket_connect";
            case TLS:
                return "tls";
            case LOGIN_EXCHANGE:
                return "login";
            case TOKEN_REQUEST:
                return "token_acquisition";
            case CONNECTION_INITIALIZE:
                return "initialize";
            case CONNECTION_REDIRECT:
                return "redirect";
            default:
                return null;
        }
    }

    boolean isLifecycleOnly() {
        return this != CONNECTION && this != PRELOGIN && connectionPhase() != null;
    }

    @Override
    public String toString() {
        return activity;
    }
}
