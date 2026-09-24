/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.cert.CertificateException;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLException;
import javax.security.auth.login.LoginException;


/** Source-evidence-only classification. Never reads messages, stack traces or lazy SQLException next chains. */
final class ConnectionTelemetryError {
    private static final int MAX_CAUSES = 32;
    final String category;
    final String source;
    final String errorType;
    final String phase;
    final Map<String, Object> attributes;

    private ConnectionTelemetryError(String category, String source, String type, String phase,
            Map<String, Object> evidence) {
        this.category = category;
        this.source = source;
        this.errorType = type;
        this.phase = safePhase(phase);
        evidence.put("mssql.error.category", category);
        evidence.put("mssql.error.phase", this.phase);
        evidence.put("mssql.error.source", source);
        attributes = Collections.unmodifiableMap(new LinkedHashMap<>(evidence));
    }

    static ConnectionTelemetryError classify(Throwable failure, String phase) {
        return classify(failure, phase, null);
    }

    static ConnectionTelemetryError classify(Throwable failure, String phase, String resourceKey) {
        return classify(failure, phase, resourceKey, false);
    }

    static ConnectionTelemetryError classify(Throwable failure, String phase, String resourceKey, boolean callback) {
        String category = "unknown";
        String source = "unknown";
        String type = "unknown";
        String code = null;
        int priority = 0;
        Map<String, Object> evidence = new LinkedHashMap<>();
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        // Inspect causes, NOT SQLException.iterator() or getNextException(): those can perform network reads.
        for (Throwable t = failure; t != null && seen.size() < MAX_CAUSES && seen.add(t); t = t.getCause()) {
            if (t instanceof SQLException) {
                String state = ((SQLException) t).getSQLState();
                if (state != null && state.matches("[A-Z0-9]{5}")) {
                    evidence.put("mssql.error.sql_state", state);
                }
            }
            if (t instanceof SQLServerException) {
                SQLServerException sql = (SQLServerException) t;
                int driverCode = sql.getDriverErrorCode();
                if (driverCode != SQLServerException.DRIVER_ERROR_NONE) {
                    evidence.put("mssql.error.driver_code", (long) driverCode);
                }
                SQLServerError server = sql.getSQLServerError();
                if (server != null && priority < 100) {
                    category = serverCategory(server.getErrorNumber());
                    source = "sql_server";
                    type = "sqlserver." + server.getErrorNumber();
                    code = "sqlserver:" + server.getErrorNumber();
                    priority = 100;
                    evidence.put("mssql.error.server_state", (long) server.getErrorState());
                    evidence.put("mssql.error.server_severity", (long) server.getErrorSeverity());
                } else if (priority < 60 && driverCode != SQLServerException.DRIVER_ERROR_NONE
                        && driverCode != SQLServerException.DRIVER_ERROR_FROM_DATABASE) {
                    category = driverCategory(driverCode);
                    source = "driver";
                    type = "jdbc_driver." + driverCode;
                    code = "jdbc_driver:" + driverCode;
                    priority = "unknown".equals(category) ? 10 : 60;
                }
            }
            String causeCategory = null;
            Class<?> safeType = null;
            int causePriority = 80;
            if (t instanceof SocketTimeoutException) {
                causeCategory = "timeout";
                safeType = SocketTimeoutException.class;
            } else if (t instanceof SQLTimeoutException) {
                causeCategory = "timeout";
                safeType = SQLTimeoutException.class;
            } else if (t instanceof TimeoutException) {
                causeCategory = "timeout";
                safeType = TimeoutException.class;
            } else if (t instanceof InterruptedException) {
                causeCategory = "canceled";
                safeType = InterruptedException.class;
            } else if (t instanceof CancellationException) {
                causeCategory = "canceled";
                safeType = CancellationException.class;
            } else if (t instanceof UnknownHostException) {
                causeCategory = "name_resolution";
                safeType = UnknownHostException.class;
            } else if (t instanceof CertificateException) {
                causeCategory = "tls_security";
                safeType = CertificateException.class;
            } else if (t instanceof SocketException) {
                causeCategory = "network_connectivity";
                safeType = SocketException.class;
                causePriority = 70;
            } else if (t instanceof EOFException) {
                causeCategory = "network_connectivity";
                safeType = EOFException.class;
                causePriority = 70;
            } else if (t instanceof OutOfMemoryError) {
                causeCategory = "client_resource_exhaustion";
                safeType = OutOfMemoryError.class;
            } else if (t instanceof ClassNotFoundException || t instanceof LinkageError
                    || t instanceof FileNotFoundException) {
                causeCategory = "configuration";
                safeType = t instanceof ClassNotFoundException ? ClassNotFoundException.class
                                                               : t instanceof LinkageError ? LinkageError.class
                                                                                           : FileNotFoundException.class;
            } else if (t instanceof SSLException) {
                causeCategory = "tls_security";
                safeType = SSLException.class;
                causePriority = 50;
            } else if (t instanceof LoginException) {
                causeCategory = "authentication";
                safeType = LoginException.class;
                causePriority = 50;
            }
            if (causeCategory != null && causePriority > priority) {
                category = causeCategory;
                source = "jvm";
                // Use a closed list of JDK types, never t.getClass().getName() (custom names can contain secrets).
                type = safeType.getName();
                code = null;
                priority = causePriority;
            }
        }
        String resourceCategory = resourceCategory(resourceKey);
        if (resourceCategory != null && priority < 70) {
            category = resourceCategory;
            source = "driver";
            type = "jdbc." + resourceKey;
            code = "jdbc:" + resourceKey;
        } else if ("unknown".equals(category) && "configuration".equals(phase)) {
            category = "configuration";
            source = "driver";
            type = "jdbc.configuration";
        }
        if (callback && "unknown".equals(source)) {
            source = "callback";
            category = "authentication";
            type = "jdbc.access_token_callback";
        }
        if (code != null) {
            evidence.put("mssql.error.code", code);
        }
        return new ConnectionTelemetryError(category, source, type, phase, evidence);
    }

    static String safePhase(String phase) {
        if (phase != null) {
            switch (phase) {
                case "configuration":
                case "instance_discovery":
                case "dns":
                case "socket_connect":
                case "prelogin":
                case "tls":
                case "login":
                case "token_acquisition":
                case "redirect":
                case "initialize":
                    return phase;
                default:
                    break;
            }
        }
        return "unknown";
    }

    private static String driverCategory(int code) {
        switch (code) {
            case SQLServerException.DRIVER_ERROR_IO_FAILED:
                return "network_connectivity";
            case SQLServerException.DRIVER_ERROR_INVALID_TDS:
                return "protocol_error";
            case SQLServerException.DRIVER_ERROR_SSL_FAILED:
            case SQLServerException.DRIVER_ERROR_INTERMITTENT_TLS_FAILED:
                return "tls_security";
            case SQLServerException.ERROR_SOCKET_TIMEOUT:
                return "timeout";
            // UNSUPPORTED_CONFIG also wraps routing budget expiration. It alone is not configuration evidence.
            default:
                return "unknown";
        }
    }

    private static String serverCategory(int code) {
        switch (code) {
            case 18456:
            case 18486:
            case 18488:
                return "authentication";
            case 40615:
                return "access_policy";
            case 40197:
            case 40143:
            case 40166:
            case 40540:
            case 40020:
            case 40501:
            case 40613:
            case 10928:
            case 10929:
            case 49918:
            case 49919:
            case 49920:
            case 4221:
            case 42108:
            case 42109:
                return "server_availability";
            case 10053:
            case 10054:
            case 64:
                return "network_connectivity";
            default:
                return "unknown";
        }
    }

    // Only explicitly captured, audited keys. Never infer a key by matching localized exception text.
    private static String resourceCategory(String key) {
        if (key == null) {
            return null;
        }
        switch (key) {
            case "R_connectionTimedOut":
            case "R_timedOutBeforeRouting":
                return "timeout";
            case "R_invalidPortNumber":
            case "R_invalidBooleanValue":
            case "R_errorConnectionString":
            case "R_invalidConnection":
            case "R_nullConnection":
            case "R_invalidSocketTimeout":
            case "R_invalidTimeOut":
            case "R_notConfiguredToListentcpip":
            case "R_notConfiguredForIntegrated":
            case "R_MSALMissing":
            case "R_DLLandMSALMissing":
            case "R_UnableLoadMSSQLAuthDll":
            case "R_InvalidAccessTokenCallbackClass":
            case "R_readCertError":
                return "configuration";
            case "R_noServerResponse":
            case "R_truncatedServerResponse":
            case "R_tcpipConnectionFailed":
            case "R_tcpOpenFailed":
            case "R_sqlBrowserFailed":
                return "network_connectivity";
            case "R_invalidTDS":
            case "R_unexpectedToken":
                return "protocol_error";
            case "R_invalidRoutingInfo":
            case "R_invalidEnhancedRoutingInfo":
            case "R_multipleRedirections":
                return "routing_redirect";
            case "R_sslFailed":
            case "R_sslRequiredNoServerSupport":
            case "R_sslRequiredByServer":
            case "R_certNameFailed":
            case "R_serverCertExpired":
            case "R_serverCertNotYetValid":
            case "R_ALPNFailed":
                return "tls_security";
            case "R_integratedAuthenticationFailed":
            case "R_kerberosLoginFailed":
            case "R_MSALExecution":
            case "R_ManagedIdentityTokenAcquisitionError":
            case "R_ManagedIdentityTokenAcquisitionFail":
                return "authentication";
            case "R_connectionIsClosed":
            case "R_physicalConnectionIsClosed":
                return "connection_lifecycle";
            case "R_crClientAllRecoveryAttemptsFailed":
            case "R_crClientNoRecoveryAckFromLogin":
            case "R_crServerSessionStateNotRecoverable":
            case "R_crClientUnrecoverable":
            case "R_crClientSSLStateNotRecoverable":
                return "connection_recovery";
            default:
                return null;
        }
    }
}
