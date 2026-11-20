/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;


/** Defense in depth: validate values as well as keys; never examine exception objects or runtime properties. */
final class ConnectionAttributePolicy {
    private static final Set<String> PHASES = values("configuration", "instance_discovery", "dns", "socket_connect",
            "prelogin", "tls", "login", "token_acquisition", "redirect", "initialize", "unknown");
    private static final Set<String> CATEGORIES = values("configuration", "name_resolution", "network_connectivity",
            "tls_security", "authentication", "access_policy", "server_availability", "protocol_error", "timeout",
            "canceled", "client_resource_exhaustion", "connection_lifecycle", "connection_recovery", "routing_redirect",
            "internal_error", "unknown");
    private static final Set<String> JDK_TYPES = values("java.net.SocketTimeoutException",
            "java.sql.SQLTimeoutException", "java.util.concurrent.TimeoutException", "java.lang.InterruptedException",
            "java.util.concurrent.CancellationException", "java.net.UnknownHostException",
            "java.security.cert.CertificateException", "java.net.SocketException", "java.io.EOFException",
            "java.lang.OutOfMemoryError", "java.lang.ClassNotFoundException", "java.lang.LinkageError",
            "java.io.FileNotFoundException", "javax.net.ssl.SSLException", "javax.security.auth.login.LoginException",
            "java.lang.IllegalStateException", "java.util.concurrent.RejectedExecutionException");
    // Audited core classifier keys, not a prefix wildcard over arbitrary resource/message text.
    private static final Set<String> RESOURCE_KEYS = values("R_connectionTimedOut", "R_timedOutBeforeRouting",
            "R_invalidPortNumber", "R_invalidBooleanValue", "R_errorConnectionString", "R_invalidConnection",
            "R_nullConnection", "R_invalidSocketTimeout", "R_invalidTimeOut", "R_notConfiguredToListentcpip",
            "R_notConfiguredForIntegrated", "R_MSALMissing", "R_DLLandMSALMissing", "R_readCertError",
            "R_InvalidAccessTokenCallbackClass", "R_UnableLoadMSSQLAuthDll", "R_sqlBrowserFailed", "R_noServerResponse",
            "R_truncatedServerResponse", "R_tcpipConnectionFailed", "R_tcpOpenFailed", "R_invalidTDS",
            "R_unexpectedToken", "R_invalidRoutingInfo", "R_invalidEnhancedRoutingInfo", "R_multipleRedirections",
            "R_sslFailed", "R_sslRequiredNoServerSupport", "R_sslRequiredByServer", "R_certNameFailed",
            "R_serverCertExpired", "R_serverCertNotYetValid", "R_ALPNFailed", "R_integratedAuthenticationFailed",
            "R_kerberosLoginFailed", "R_MSALExecution", "R_ManagedIdentityTokenAcquisitionError",
            "R_ManagedIdentityTokenAcquisitionFail", "R_connectionIsClosed", "R_physicalConnectionIsClosed",
            "R_crClientAllRecoveryAttemptsFailed", "R_crClientNoRecoveryAckFromLogin",
            "R_crServerSessionStateNotRecoverable", "R_crClientUnrecoverable", "R_crClientSSLStateNotRecoverable");

    private ConnectionAttributePolicy() {}

    private static Set<String> values(String... values) {
        return new HashSet<>(Arrays.asList(values));
    }

    static String phase(String value) {
        return PHASES.contains(value) ? value : "unknown";
    }

    static String category(Object value) {
        return CATEGORIES.contains(value) ? (String) value : "unknown";
    }

    static String errorType(Object value) {
        String text = value instanceof String ? (String) value : "unknown";
        return JDK_TYPES.contains(text) || "jdbc.configuration".equals(text)
                || "jdbc.access_token_callback".equals(text) || numericCode(text, ".")
                || (text.startsWith("jdbc.") && RESOURCE_KEYS.contains(text.substring(5))) ? text : "unknown";
    }

    private static boolean numericCode(String value, String separator) {
        String escaped = ".".equals(separator) ? "\\." : separator;
        return value.length() <= 32 && value.matches("(?:sqlserver|jdbc_driver)" + escaped + "[0-9]{1,10}");
    }

    static boolean validUserAgent(String value) {
        return value != null && value.length() <= 512 && value.matches(
                "1\\|MS-JDBC\\|(?:[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+(?:-preview)?|Unknown)(?:\\|[ -~&&[^|]]{1,128}){4}");
    }

    static Attributes span(Map<String, Object> input, boolean root, boolean attempt, String phase,
            String approvedUserAgent) {
        AttributesBuilder output = Attributes.builder().put("db.system.name", "microsoft.sql_server");
        if (input.containsKey("mssql.error.category")) {
            output.put("mssql.error.category", category(input.get("mssql.error.category")));
        }
        if (input.containsKey("error.type")) {
            output.put("error.type", errorType(input.get("error.type")));
        }
        if (!root) {
            integer(input, output, "mssql.connection.attempt", Integer.MAX_VALUE);
            enumeration(input, output, "mssql.connection.endpoint_role", "database", "gateway", "redirect_target",
                    "sql_browser", "identity_provider", "unknown");
        }
        if (root || "login".equals(phase) || "token_acquisition".equals(phase)) {
            authentication(input, output);
        }
        if ("token_acquisition".equals(phase)) {
            tokenSource(input, output);
        }
        if (attempt) {
            uuid(input, output, "mssql.connection.client_connection_id");
            enumeration(input, output, "mssql.connection.attempt_reason", "initial", "retry", "redirect", "failover");
            enumeration(input, output, "mssql.connection.attempt_outcome", "success", "failure", "timeout", "canceled",
                    "redirect");
            bool(input, output, "mssql.connection.transparent_network_ip_resolution");
            enumeration(input, output, "mssql.connection.tds_version", "7.0", "7.1", "7.2", "7.3", "7.4", "8.0");
        }
        if ("socket_connect".equals(phase)) {
            enumeration(input, output, "mssql.connection.transport_strategy", "serial", "parallel", "tnir");
        }
        if ("tls".equals(phase)) {
            enumeration(input, output, "mssql.connection.tls_version", "TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3");
        }
        if (root) {
            uuid(input, output, "mssql.connection.guid");
            enumeration(input, output, "mssql.telemetry.schema.version", "1.0");
            enumeration(input, output, "mssql.connection.origin", "driver", "datasource", "pooled_physical",
                    "xa_control");
            enumeration(input, output, "mssql.connection.outcome", "success", "failure", "timeout", "canceled");
            if (input.containsKey("mssql.connection.failure_phase")) {
                Object value = input.get("mssql.connection.failure_phase");
                output.put("mssql.connection.failure_phase", phase(value instanceof String ? (String) value : null));
            }
            enumeration(input, output, "mssql.connection.encrypt", "false", "true", "strict");
            enumeration(input, output, "mssql.connection.application_intent", "read_only", "read_write");
            for (String key : Arrays.asList("mssql.connection.trust_server_certificate",
                    "mssql.connection.multi_subnet_failover", "mssql.connection.transparent_network_ip_resolution",
                    "mssql.connection.budget_exhausted")) {
                bool(input, output, key);
            }
            for (String key : Arrays.asList("mssql.connection.login_timeout", "mssql.connection.socket_timeout",
                    "mssql.connection.connect_retry_interval")) {
                seconds(input, output, key);
            }
            integer(input, output, "mssql.connection.connect_retry_count", 255);
            integer(input, output, "mssql.connection.attempt_count", Integer.MAX_VALUE);
            integer(input, output, "mssql.connection.retry_count", Integer.MAX_VALUE);
            integer(input, output, "mssql.connection.redirect_count", Integer.MAX_VALUE);
            integer(input, output, "mssql.connection.diagnostic_events_dropped", Long.MAX_VALUE);
            if (approvedUserAgent != null && approvedUserAgent.equals(input.get("mssql.driver.user_agent.original"))) {
                output.put("mssql.driver.user_agent.original", approvedUserAgent);
            }
        }
        return output.build();
    }

    static Attributes error(Map<String, Object> input) {
        AttributesBuilder output = Attributes.builder();
        output.put("mssql.error.category", category(input.get("mssql.error.category")));
        Object phase = input.get("mssql.error.phase");
        // Lifecycle diagnostics may identify these phases, but this adapter does not create reset/recovery roots
        // or treat them as physical-open/timeout phases.
        output.put("mssql.error.phase", "reset".equals(phase)
                || "recovery".equals(phase) ? (String) phase : phase(phase instanceof String ? (String) phase : null));
        Object source = input.get("mssql.error.source");
        output.put("mssql.error.source",
                values("driver", "sql_server", "jvm", "os", "identity_provider", "callback", "external_pool", "unknown")
                        .contains(source) ? (String) source : "unknown");
        Object code = input.get("mssql.error.code");
        if (code instanceof String && (numericCode((String) code, ":")
                || (((String) code).startsWith("jdbc:") && RESOURCE_KEYS.contains(((String) code).substring(5))))) {
            output.put("mssql.error.code", (String) code);
        }
        Object type = input.get("exception.type");
        if (JDK_TYPES.contains(type)) {
            output.put("exception.type", (String) type);
        }
        Object state = input.get("mssql.error.sql_state");
        if (state instanceof String && ((String) state).matches("[A-Z0-9]{5}")) {
            output.put("mssql.error.sql_state", (String) state);
        }
        integer(input, output, "mssql.error.driver_code", Integer.MAX_VALUE);
        integer(input, output, "mssql.error.server_state", 255);
        integer(input, output, "mssql.error.server_severity", 25);
        enumeration(input, output, "mssql.error.retry_decision", "retry_scheduled", "not_retryable", "limit_reached",
                "budget_exhausted", "canceled");
        return output.build();
    }

    // Independently validate core diagnostics; never copy arbitrary maps or provider messages.
    static Attributes diagnostic(String name, Map<String, Object> input) {
        AttributesBuilder output = Attributes.builder();
        if ("mssql.driver.authentication".equals(name)) {
            authentication(input, output);
            tokenSource(input, output);
            integer(input, output, "mssql.connection.attempt", Integer.MAX_VALUE);
        } else if ("mssql.driver.retry".equals(name) || "mssql.driver.connection.retry_decision".equals(name)) {
            integer(input, output, "mssql.connection.attempt", Integer.MAX_VALUE);
            integer(input, output, "mssql.retry.attempt", Integer.MAX_VALUE);
            seconds(input, output, "mssql.retry.delay");
            enumeration(input, output, "mssql.error.retry_decision", "retry_scheduled", "not_retryable",
                    "limit_reached", "budget_exhausted", "canceled");
            // The cross-driver reason registry is not finalized. Omit rather than export provider/message text.
            errorType(input, output);
        } else if ("mssql.driver.redirect".equals(name)) {
            integer(input, output, "mssql.connection.attempt", Integer.MAX_VALUE);
            integer(input, output, "mssql.connection.redirect.index", Integer.MAX_VALUE);
            enumeration(input, output, "mssql.connection.redirect.type", "tds_routing", "enhanced_routing");
            enumeration(input, output, "mssql.connection.endpoint_role", "redirect_target");
        } else if ("mssql.driver.timeout".equals(name)) {
            Object phase = input.get("mssql.timeout.phase");
            if (phase instanceof String && PHASES.contains(phase)) {
                output.put("mssql.timeout.phase", (String) phase);
            }
            seconds(input, output, "mssql.timeout.value");
            enumeration(input, output, "mssql.timeout.kind", "login_budget", "socket_read", "socket_selection",
                    "socket_connect", "token_request", "routing_budget", "unknown");
            integer(input, output, "mssql.connection.attempt", Integer.MAX_VALUE);
            errorType(input, output);
        } else {
            return null;
        }
        return output.build();
    }

    private static void authentication(Map<String, Object> input, AttributesBuilder output) {
        enumeration(input, output, "mssql.authentication.method", "unknown", "sql_password", "entra_password",
                "entra_integrated", "managed_identity", "service_principal_secret", "service_principal_certificate",
                "interactive", "default_credential", "integrated_kerberos", "integrated_ntlm", "integrated_native",
                "access_token", "access_token_callback");
    }

    private static void tokenSource(Map<String, Object> input, AttributesBuilder output) {
        enumeration(input, output, "mssql.authentication.token_source", "msal", "native", "managed_identity",
                "default_credential", "callback");
    }

    private static void errorType(Map<String, Object> input, AttributesBuilder output) {
        if (input.containsKey("error.type")) {
            output.put("error.type", errorType(input.get("error.type")));
        }
    }

    private static void bool(Map<String, Object> input, AttributesBuilder output, String key) {
        if (input.get(key) instanceof Boolean) {
            output.put(key, (Boolean) input.get(key));
        }
    }

    private static void seconds(Map<String, Object> input, AttributesBuilder output, String key) {
        Object value = input.get(key);
        if (value instanceof Double || value instanceof Long || value instanceof Integer) {
            double number = ((Number) value).doubleValue();
            if (Double.isFinite(number) && number >= 0 && number <= Integer.MAX_VALUE) {
                output.put(key, number);
            }
        }
    }

    private static void enumeration(Map<String, Object> input, AttributesBuilder output, String key,
            String... allowed) {
        Object value = input.get(key);
        if (Arrays.asList(allowed).contains(value)) {
            output.put(key, (String) value);
        }
    }

    private static void uuid(Map<String, Object> input, AttributesBuilder output, String key) {
        Object value = input.get(key);
        if (value instanceof String && ((String) value)
                .matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")) {
            output.put(key, (String) value);
        }
    }

    private static void integer(Map<String, Object> input, AttributesBuilder output, String key, long max) {
        Object value = input.get(key);
        if (value instanceof Long || value instanceof Integer) {
            long number = ((Number) value).longValue();
            if (number >= 0 && number <= max) {
                output.put(key, number);
            }
        }
    }
}
