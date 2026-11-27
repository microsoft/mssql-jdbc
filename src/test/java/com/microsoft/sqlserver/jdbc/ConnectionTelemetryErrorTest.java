/*
 * Microsoft JDBC Driver for SQL Server
 * Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.io.EOFException;
import java.lang.reflect.Field;
import java.net.BindException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.cert.CertificateException;
import java.sql.SQLException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLException;
import javax.security.auth.login.LoginException;

import org.junit.jupiter.api.Test;


class ConnectionTelemetryErrorTest {
    @Test
    void auditedLocalSourcesDoNotFallBackToUnknownOrSocketConfiguration() {
        assertEquals("network_connectivity", ConnectionTelemetryError.classify(new java.io.IOException(),
                "instance_discovery", "R_sqlBrowserFailed").category);
        assertEquals("configuration",
                ConnectionTelemetryError.classify(new ClassNotFoundException(), "socket_connect").category);
        assertEquals("configuration", ConnectionTelemetryError.classify(new SQLException(), "token_acquisition",
                "R_UnableLoadMSSQLAuthDll").category);
        assertEquals("configuration",
                ConnectionTelemetryError.classify(new SQLException(), "token_acquisition", "R_MSALMissing").category);
        assertEquals("timeout", ConnectionTelemetryError.classify(new java.io.IOException(), "socket_connect",
                "R_connectionTimedOut").category);
        assertEquals("timeout",
                ConnectionTelemetryError.classify(new SQLException(), "redirect", "R_timedOutBeforeRouting").category);
    }

    @Test
    void canonicalResourceCategoriesAndContextualSources() {
        assertEquals("routing_redirect",
                ConnectionTelemetryError.classify(new SQLException(), "redirect", "R_multipleRedirections").category);
        assertEquals("unknown",
                ConnectionTelemetryError.classify(new SQLException(), "login", "R_internalError").category);
        assertEquals("configuration", ConnectionTelemetryError.classify(new SQLException(), "token_acquisition",
                "R_InvalidAccessTokenCallbackClass").category);
        assertEquals("jvm", ConnectionTelemetryError.classify(new UnknownHostException(), "dns").source);
        assertEquals("jvm", ConnectionTelemetryError.classify(new SocketTimeoutException(), "socket_connect").source);
        assertEquals("jvm", ConnectionTelemetryError.classify(new EOFException(), "prelogin").source);
    }

    @Test
    void concreteCauseEvidenceBeatsBroadWrappers() {
        assertCategory("name_resolution", new UnknownHostException("SECRET"), "socket_connect");
        assertCategory("network_connectivity", new EOFException(), "prelogin");
        assertCategory("timeout", new SocketTimeoutException(), "tls");
        assertCategory("timeout", new TimeoutException(), "token_acquisition");
        assertCategory("canceled", new InterruptedException(), "login");
        assertCategory("canceled", new CancellationException(), "token_acquisition");
        assertCategory("tls_security", new CertificateException(), "tls");
        SSLException wrapper = new SSLException("SECRET");
        wrapper.initCause(new SocketTimeoutException("SECRET"));
        assertCategory("timeout", wrapper, "tls");
        assertCategory("authentication", new LoginException(), "login");
        assertCategory("client_resource_exhaustion", new OutOfMemoryError(), "socket_connect");
    }

    @Test
    void noTextOrResourceGuessingAndNoLazySqlChainReads() {
        assertCategory("unknown", new RuntimeException("out of memory timeout firewall denied"), "login");
        assertCategory("unknown", new RejectedExecutionException(), "initialize");
        assertCategory("network_connectivity", new BindException(), "socket_connect");
        assertCategory("unknown", new SQLException("SECRET", "08S01", 4060), "login");
        assertCategory("unknown", new SQLServerException("SECRET", "28000", 18456, null), "login");
        SQLException lazy = new SQLException() {
            private static final long serialVersionUID = 1L;

            @Override
            public SQLException getNextException() {
                fail("Classifier must never perform lazy server reads");
                return null;
            }
        };
        assertCategory("unknown", lazy, "login");
        assertCategory("configuration", new SQLException(), "configuration");
        assertCategory("unknown", new IllegalArgumentException(), "login");
    }

    @Test
    void actualServerResponseHasPriority() throws Exception {
        int[] codes = {18456, 18488, 40615, 40613, 10928, 10054, 4060, 233};
        String[] categories = {"authentication", "authentication", "access_policy", "server_availability",
                "server_availability", "network_connectivity", "unknown", "unknown"};
        for (int i = 0; i < codes.length; i++) {
            SQLServerError serverError = new SQLServerError();
            set(serverError, "errorNumber", codes[i]);
            set(serverError, "errorState", 7);
            set(serverError, "errorSeverity", 14);
            SQLServerException exception = new SQLServerException(serverError);
            ConnectionTelemetryError result = ConnectionTelemetryError.classify(new SQLException("SECRET", exception),
                    "login");
            assertEquals(categories[i], result.category);
            assertEquals("sql_server", result.source);
            assertEquals("sqlserver." + codes[i], result.errorType);
            assertEquals("sqlserver:" + codes[i], result.attributes.get("mssql.error.code"));
            assertEquals(7L, result.attributes.get("mssql.error.server_state"));
            assertEquals(14L, result.attributes.get("mssql.error.server_severity"));
        }
    }

    @Test
    void driverCodesAreNotVendorNumbers() {
        SQLServerException exception = new SQLServerException("SECRET", (Throwable) null);
        exception.setDriverErrorCode(SQLServerException.DRIVER_ERROR_INVALID_TDS);
        ConnectionTelemetryError result = ConnectionTelemetryError.classify(exception, "prelogin");
        assertEquals("protocol_error", result.category);
        assertEquals("driver", result.source);
        assertEquals("jdbc_driver:4", result.attributes.get("mssql.error.code"));
        assertEquals(4L, result.attributes.get("mssql.error.driver_code"));
        assertFalse(result.attributes.containsKey("mssql.error.sql_state"));
        exception.setDriverErrorCode(SQLServerException.DRIVER_ERROR_UNSUPPORTED_CONFIG);
        assertCategory("unknown", exception, "redirect");
    }

    @Test
    void causesAreBoundedCycleSafeAndCustomClassNamesAreNotExported() {
        RuntimeException first = new RuntimeException();
        RuntimeException second = new RuntimeException();
        first.initCause(second);
        second.initCause(first);
        assertCategory("unknown", first, "login");
        Throwable chain = new UnknownHostException();
        for (int i = 0; i < 200; i++) {
            chain = new RuntimeException(chain);
        }
        assertCategory("unknown", chain, "login");
        ConnectionTelemetryError result = ConnectionTelemetryError.classify(new SecretException(), "SECRET");
        assertEquals("unknown", result.phase);
        assertFalse(result.attributes.toString().contains("SecretException"));
        assertFalse(result.errorType.contains("SecretException"));
    }

    private static final class SecretException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private static void assertCategory(String expected, Throwable failure, String phase) {
        ConnectionTelemetryError result = ConnectionTelemetryError.classify(failure, phase);
        assertEquals(expected, result.category);
        assertFalse(result.attributes.toString().contains("SECRET"));
    }

    private static void set(SQLServerError error, String name, int value) throws Exception {
        Field field = SQLServerError.class.getDeclaredField(name);
        field.setAccessible(true);
        field.setInt(error, value);
    }
}
