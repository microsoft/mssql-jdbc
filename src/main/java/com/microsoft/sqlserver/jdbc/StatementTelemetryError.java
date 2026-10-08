/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;


/** Source-evidence-only statement error projection. Never examines localized exception messages. */
final class StatementTelemetryError {
    final String phase;
    final String category;
    final String errorType;
    final Map<String, Object> attributes;

    private StatementTelemetryError(String phase, String category, String errorType, Map<String, Object> attributes) {
        this.phase = safePhase(phase);
        this.category = category;
        this.errorType = errorType;
        this.attributes = Collections.unmodifiableMap(attributes);
    }

    static StatementTelemetryError classify(Exception exception, String phase, String resourceKey) {
        String category = "unknown";
        String type = exception == null ? "unknown" : exception.getClass().getName();
        String source = "unknown";
        Map<String, Object> attributes = new LinkedHashMap<>();
        SQLServerException sqlException = findSqlServerException(exception);
        if (sqlException != null) {
            SQLServerError server = sqlException.getSQLServerError();
            if (server != null) {
                int number = server.getErrorNumber();
                category = serverCategory(number);
                type = "sqlserver." + number;
                source = "sql_server";
                attributes.put("mssql.error.code", "sqlserver:" + number);
                attributes.put("mssql.error.server_state", (long) server.getErrorState());
                attributes.put("mssql.error.server_severity", (long) server.getErrorSeverity());
            } else if (sqlException.getDriverErrorCode() == SQLServerException.ERROR_QUERY_TIMEOUT) {
                category = "timeout";
                source = "driver";
                type = "java.sql.SQLTimeoutException";
            } else if (sqlException.getDriverErrorCode() == SQLServerException.ERROR_SOCKET_TIMEOUT) {
                category = "timeout";
                source = "jvm";
                type = "java.net.SocketTimeoutException";
            } else if (sqlException.getDriverErrorCode() == SQLServerException.DRIVER_ERROR_IO_FAILED) {
                category = "network_connectivity";
                source = "jvm";
            } else if (sqlException.getDriverErrorCode() == SQLServerException.DRIVER_ERROR_INVALID_TDS) {
                category = "protocol_error";
                source = "driver";
            }
            attributes.put("mssql.error.driver_code", (long) sqlException.getDriverErrorCode());
        }
        if (exception instanceof SQLTimeoutException || hasCause(exception, SocketTimeoutException.class)) {
            category = "timeout";
            source = exception instanceof SQLTimeoutException ? "driver" : "jvm";
            type = exception instanceof SQLTimeoutException ? "java.sql.SQLTimeoutException"
                                                             : "java.net.SocketTimeoutException";
        } else if (hasCause(exception, SocketException.class) && "unknown".equals(category)) {
            category = "network_connectivity";
            source = "jvm";
            type = "java.net.SocketException";
        }
        if (exception instanceof SQLException) {
            String state = ((SQLException) exception).getSQLState();
            if (state != null && state.matches("[A-Z0-9]{5}")) {
                attributes.put("mssql.error.sql_state", state);
            }
        }
        if (resourceKey != null) {
            String mapped = resourceCategory(resourceKey);
            if (mapped != null && "unknown".equals(category)) {
                category = mapped;
                source = "driver";
                type = "jdbc." + resourceKey;
                attributes.put("mssql.error.code", "jdbc:" + resourceKey);
            }
        }
        attributes.put("mssql.error.category", category);
        attributes.put("mssql.error.phase", safePhase(phase));
        attributes.put("mssql.error.source", source);
        attributes.put("exception.type", type);
        return new StatementTelemetryError(phase, category, type, attributes);
    }

    private static SQLServerException findSqlServerException(Throwable error) {
        for (int i = 0; error != null && i < 8; i++, error = error.getCause()) {
            if (error instanceof SQLServerException) {
                return (SQLServerException) error;
            }
        }
        return null;
    }

    private static boolean hasCause(Throwable error, Class<?> type) {
        for (int i = 0; error != null && i < 8; i++, error = error.getCause()) {
            if (type.isInstance(error)) {
                return true;
            }
        }
        return false;
    }

    private static String serverCategory(int number) {
        switch (number) {
            case 102:
            case 156:
            case 207:
            case 208:
            case 2812:
                return "query_syntax_semantics";
            case 515:
            case 547:
            case 2601:
            case 2627:
                return "constraint_violation";
            case 229:
            case 230:
                return "authorization";
            case 1205:
            case 1222:
                return "concurrency_conflict";
            case 40143:
            case 40166:
            case 40197:
            case 40540:
            case 40613:
            case 4221:
            case 42108:
            case 42109:
                return "server_availability";
            case 40501:
            case 10928:
            case 10929:
            case 49918:
            case 49919:
            case 49920:
                return "resource_throttling";
            case 64:
            case 10053:
            case 10054:
                return "network_connectivity";
            default:
                return "database_error";
        }
    }

    private static String resourceCategory(String key) {
        if ("R_noResultset".equals(key) || "R_resultsetGeneratedForUpdate".equals(key)
                || "R_updateCountOutofRange".equals(key)) {
            return "result_contract";
        }
        if ("R_valueNotSetForParameter".equals(key) || "R_invalidParameterLength".equals(key)
                || "R_defineParameterTypeTypeMismatch".equals(key) || "R_TVPInvalidValue".equals(key)) {
            return "parameter_validation";
        }
        if ("R_invalidTDS".equals(key) || "R_unexpectedToken".equals(key)) {
            return "protocol_error";
        }
        if ("R_statementIsClosed".equals(key) || "R_connectionIsClosed".equals(key)) {
            return "connection_lifecycle";
        }
        if ("R_queryTimedOut".equals(key)) {
            return "timeout";
        }
        return null;
    }

    private static String safePhase(String phase) {
        return "request_build".equals(phase) || "server_call".equals(phase) || "first_response".equals(phase)
                || "attempt".equals(phase) ? phase : "unknown";
    }
}
