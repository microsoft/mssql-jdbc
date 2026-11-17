/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;


/** Validates only user telemetry options; never retains SQL credentials or initializes an SDK. */
final class UserOtelConfiguration {
    private UserOtelConfiguration() {}

    static Properties validate(Properties properties) throws SQLServerException {
        Properties options = new Properties();
        try {
            String endpoint = properties.getProperty(SQLServerDriverStringProperty.OTEL_ENDPOINT.toString());
            if (endpoint != null) {
                URI uri = new URI(endpoint.trim());
                if (!("https".equalsIgnoreCase(uri.getScheme()) || "http".equalsIgnoreCase(uri.getScheme()))
                        || uri.getHost() == null || uri.getRawUserInfo() != null || uri.getRawQuery() != null
                        || uri.getRawFragment() != null || uri.getPort() > 65535 || uri.getPort() == 0) {
                    throw new IllegalArgumentException();
                }
                options.setProperty(SQLServerDriverStringProperty.OTEL_ENDPOINT.toString(),
                        uri.normalize().toASCIIString());
            }
            String levelName = SQLServerDriverIntProperty.OTEL_LOG_LEVEL.toString();
            int level = Integer.parseInt(properties.getProperty(levelName, "0").trim());
            if (!SQLServerDriverIntProperty.OTEL_LOG_LEVEL.isValidValue(level)) {
                throw new IllegalArgumentException();
            }
            options.setProperty(levelName, Integer.toString(level));
            String callbackName = SQLServerDriverStringProperty.OTEL_ACCESS_TOKEN_CALLBACK.toString();
            String callback = properties.getProperty(callbackName);
            if (callback != null) {
                callback = callback.trim();
                if (!callback.matches(
                        "[\\p{javaJavaIdentifierStart}][\\p{javaJavaIdentifierPart}]*(\\.[\\p{javaJavaIdentifierStart}][\\p{javaJavaIdentifierPart}]*)*")) {
                    throw new IllegalArgumentException();
                }
                options.setProperty(callbackName, callback);
            }
            threshold(properties, options, SQLServerDriverStringProperty.OTEL_CONNECTION_SPAN_DURATION_THRESHOLD);
            threshold(properties, options, SQLServerDriverStringProperty.OTEL_STATEMENT_SPAN_DURATION_THRESHOLD);
        } catch (IllegalArgumentException | ArithmeticException | URISyntaxException e) {
            // Neither the value nor the cause is safe to include in diagnostics.
            SQLServerException.makeFromDriverError(null, null,
                    SQLServerException.getErrString("R_invalidUserOtelConfiguration"), null, false);
        }
        return options;
    }

    private static void threshold(Properties properties, Properties options, SQLServerDriverStringProperty property) {
        String value = properties.getProperty(property.toString());
        if (value != null) {
            BigDecimal seconds = new BigDecimal(value.trim());
            if (seconds.signum() < 0
                    || seconds.scaleByPowerOfTen(9).compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
                throw new IllegalArgumentException();
            }
            options.setProperty(property.toString(), seconds.stripTrailingZeros().toString());
        }
    }

    static PerformanceLog.Registration create(Properties properties) throws SQLServerException {
        // Avoid allocating options or loading ServiceLoader/SDK classes on the normal non-opt-in path.
        if (!isConfigured(properties)) {
            return null;
        }
        Properties options = validate(properties);
        if (!options.containsKey(SQLServerDriverStringProperty.OTEL_ENDPOINT.toString())) {
            return null;
        }
        try {
            for (UserOtelTelemetryProvider provider : ServiceLoader.load(UserOtelTelemetryProvider.class)) {
                PerformanceLogCallback callback = provider.create((Properties) options.clone());
                if (callback != null) {
                    return new PerformanceLog.Registration(callback, false);
                }
            }
        } catch (Exception | LinkageError | ServiceConfigurationError e) {
            // Optional instrumentation is fail-open. Do not log provider messages or configuration.
        }
        return null;
    }

    private static boolean isConfigured(Properties properties) {
        return properties != null && (properties.getProperty("otelEndpoint") != null
                || properties.getProperty("otelAccessTokenCallback") != null
                || properties.getProperty("otelLogLevel") != null
                || properties.getProperty("otelConnectionSpanDurationThreshold") != null
                || properties.getProperty("otelStatementSpanDurationThreshold") != null);
    }
}
