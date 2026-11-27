/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import static org.junit.jupiter.api.Assertions.*;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.ConnectionEventFixture;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;


class ConnectionAttributePolicyTest {
    @Test
    void coreSocketTimeoutKindsArePreservedWithoutAllowingArbitraryValues() {
        Map<String, Object> input = new HashMap<>();
        input.put("mssql.timeout.phase", "socket_connect");
        input.put("mssql.timeout.value", 0.25);
        input.put("mssql.error.message", "SECRET");
        for (String kind : new String[] {"socket_selection", "socket_connect"}) {
            input.put("mssql.timeout.kind", kind);
            Attributes result = ConnectionAttributePolicy.diagnostic("mssql.driver.timeout", input);
            assertEquals(kind, result.get(AttributeKey.stringKey("mssql.timeout.kind")));
            assertEquals("socket_connect", result.get(AttributeKey.stringKey("mssql.timeout.phase")));
            assertEquals(0.25, result.get(AttributeKey.doubleKey("mssql.timeout.value")));
            assertEquals(3, result.size());
        }
        for (Object kind : new Object[] {"socket_connect SECRET", "socket_selection.SECRET", 1, null}) {
            input.put("mssql.timeout.kind", kind);
            assertNull(ConnectionAttributePolicy.diagnostic("mssql.driver.timeout", input)
                    .get(AttributeKey.stringKey("mssql.timeout.kind")));
        }
    }

    @Test
    void auditedCoreResourceTypesAndCodesSurvivePolicy() {
        for (String key : new String[] {"R_InvalidAccessTokenCallbackClass", "R_UnableLoadMSSQLAuthDll",
                "R_sqlBrowserFailed"}) {
            SQLException failure = new SQLException("SECRET");
            String type = ConnectionEventFixture.classifiedErrorType(failure, "configuration", key, false);
            assertEquals("jdbc." + key, type);
            assertEquals(type, ConnectionAttributePolicy.errorType(type));
            Attributes error = ConnectionAttributePolicy
                    .error(ConnectionEventFixture.classifiedErrorAttributes(failure, "configuration", key, false));
            assertEquals("jdbc:" + key, error.get(AttributeKey.stringKey("mssql.error.code")));
            assertFalse(error.toString().contains("SECRET"));
        }
        for (String key : new String[] {"R_internalError", "R_SECRET", "R_sqlBrowserFailed SECRET"}) {
            assertEquals("unknown", ConnectionAttributePolicy.errorType("jdbc." + key));
            Map<String, Object> input = new HashMap<>();
            input.put("mssql.error.code", "jdbc:" + key);
            assertNull(ConnectionAttributePolicy.error(input).get(AttributeKey.stringKey("mssql.error.code")));
        }
    }

    @Test
    void coreCallbackTypeAndCanonicalJvmSourceSurvivePolicy() {
        SQLException failure = new SQLException("SECRET callback provider");
        String type = ConnectionEventFixture.classifiedErrorType(failure, "token_acquisition", null, true);
        assertEquals("jdbc.access_token_callback", type);
        assertEquals(type, ConnectionAttributePolicy.errorType(type));
        Attributes error = ConnectionAttributePolicy
                .error(ConnectionEventFixture.classifiedErrorAttributes(failure, "token_acquisition", null, true));
        assertEquals("callback", error.get(AttributeKey.stringKey("mssql.error.source")));
        assertEquals("authentication", error.get(AttributeKey.stringKey("mssql.error.category")));
        assertFalse(error.toString().contains("SECRET"));
        assertEquals("unknown", ConnectionAttributePolicy.errorType("jdbc.access_token_callback.SECRET"));
        assertEquals("unknown", ConnectionAttributePolicy.errorType("jdbc.SECRET"));
        error = ConnectionAttributePolicy.error(ConnectionEventFixture
                .classifiedErrorAttributes(new UnknownHostException("SECRET hostname"), "dns", null, false));
        assertEquals("jvm", error.get(AttributeKey.stringKey("mssql.error.source")));
        assertFalse(error.toString().contains("SECRET"));
    }

    @Test
    void lifecycleErrorPhasesDoNotBecomePhysicalOpenOrTimeoutPhases() {
        for (String phase : new String[] {"reset", "recovery"}) {
            Map<String, Object> input = new HashMap<>();
            input.put("mssql.error.phase", phase);
            input.put("mssql.connection.failure_phase", phase);
            input.put("mssql.timeout.phase", phase);
            assertEquals(phase,
                    ConnectionAttributePolicy.error(input).get(AttributeKey.stringKey("mssql.error.phase")));
            assertEquals("unknown", ConnectionAttributePolicy.span(input, true, false, "open", null)
                    .get(AttributeKey.stringKey("mssql.connection.failure_phase")));
            assertTrue(ConnectionAttributePolicy.diagnostic("mssql.driver.timeout", input).isEmpty());
        }
    }

    @Test
    void allSixteenCategoriesArePreserved() {
        for (String category : new String[] {"name_resolution", "network_connectivity", "tls_security",
                "authentication", "access_policy", "routing_redirect", "server_availability", "configuration",
                "timeout", "canceled", "protocol_error", "connection_lifecycle", "connection_recovery",
                "client_resource_exhaustion", "internal_error", "unknown"}) {
            assertEquals(category, ConnectionAttributePolicy.category(category));
        }
        assertEquals("unknown", ConnectionAttributePolicy.category("SECRET"));
    }

    @Test
    void settingsArePlacedOnlyOnTheirContractOwners() {
        Map<String, Object> input = new HashMap<>();
        input.put("mssql.connection.retry_count", 3L);
        input.put("mssql.connection.redirect_count", 2L);
        input.put("mssql.connection.attempt", 6L);
        input.put("mssql.connection.attempt_reason", "redirect");
        input.put("mssql.connection.attempt_outcome", "failure");
        input.put("mssql.authentication.method", "access_token_callback");
        input.put("mssql.authentication.token_source", "callback");
        Attributes root = ConnectionAttributePolicy.span(input, true, false, "open", null);
        Attributes attempt = ConnectionAttributePolicy.span(input, false, true, "attempt", null);
        Attributes token = ConnectionAttributePolicy.span(input, false, false, "token_acquisition", null);
        Attributes dns = ConnectionAttributePolicy.span(input, false, false, "dns", null);
        assertEquals(3L, root.get(AttributeKey.longKey("mssql.connection.retry_count")));
        assertEquals(2L, root.get(AttributeKey.longKey("mssql.connection.redirect_count")));
        assertNull(root.get(AttributeKey.longKey("mssql.connection.attempt")));
        assertEquals("redirect", attempt.get(AttributeKey.stringKey("mssql.connection.attempt_reason")));
        assertEquals("failure", attempt.get(AttributeKey.stringKey("mssql.connection.attempt_outcome")));
        assertEquals(6L, token.get(AttributeKey.longKey("mssql.connection.attempt")));
        assertEquals("callback", token.get(AttributeKey.stringKey("mssql.authentication.token_source")));
        assertEquals("access_token_callback", token.get(AttributeKey.stringKey("mssql.authentication.method")));
        assertNull(dns.get(AttributeKey.stringKey("mssql.authentication.method")));
        assertNull(token.get(AttributeKey.stringKey("mssql.connection.attempt_reason")));
        assertNull(token.get(AttributeKey.longKey("mssql.connection.retry_count")));
    }

    @Test
    void diagnosticEventsHaveIndependentNameAndValueAllowlists() {
        Map<String, Object> input = new HashMap<>();
        input.put("server.address", "SECRET");
        input.put("mssql.error.message", "SECRET");
        input.put("mssql.authentication.method", "access_token_callback");
        input.put("mssql.authentication.token_source", "callback");
        input.put("mssql.connection.attempt", 1L);
        input.put("mssql.retry.attempt", 2L);
        input.put("mssql.retry.reason", "SECRET");
        input.put("mssql.retry.delay", 0.25);
        input.put("mssql.connection.redirect.index", 1L);
        input.put("mssql.connection.redirect.type", "tds_routing");
        input.put("mssql.connection.endpoint_role", "redirect_target");
        input.put("mssql.timeout.phase", "token_acquisition");
        input.put("mssql.timeout.value", 2.5);
        input.put("mssql.timeout.kind", "token_request");
        input.put("error.type", "java.util.concurrent.TimeoutException");
        for (String name : new String[] {"retry", "redirect", "authentication", "timeout"}) {
            Attributes result = ConnectionAttributePolicy.diagnostic("mssql.driver." + name, input);
            assertNotNull(result);
            assertFalse(result.toString().contains("SECRET"));
        }
        Attributes retry = ConnectionAttributePolicy.diagnostic("mssql.driver.retry", input);
        assertEquals(0.25, retry.get(AttributeKey.doubleKey("mssql.retry.delay")));
        assertNull(retry.get(AttributeKey.stringKey("mssql.authentication.method")));
        assertNull(retry.get(AttributeKey.stringKey("mssql.retry.reason")));
        Attributes auth = ConnectionAttributePolicy.diagnostic("mssql.driver.authentication", input);
        assertEquals(3, auth.size());
        assertEquals(1L, auth.get(AttributeKey.longKey("mssql.connection.attempt")));
        assertNull(ConnectionAttributePolicy.diagnostic("mssql.driver.SECRET", input));
        input.put("mssql.timeout.value", Double.POSITIVE_INFINITY);
        assertNull(ConnectionAttributePolicy.diagnostic("mssql.driver.timeout", input)
                .get(AttributeKey.doubleKey("mssql.timeout.value")));
    }

    @Test
    void newMetadataRejectsProviderTextWrongTypesAndOutOfRangeValues() {
        Map<String, Object> input = new HashMap<>();
        input.put("mssql.connection.retry_count", -1L);
        input.put("mssql.connection.redirect_count", Long.MAX_VALUE);
        input.put("mssql.connection.attempt", 1.5);
        input.put("mssql.connection.attempt_reason", "SECRET provider reason");
        input.put("mssql.connection.attempt_outcome", "SECRET provider outcome");
        input.put("mssql.authentication.method", "SECRET user");
        input.put("mssql.authentication.token_source", "SECRET identity URL");
        for (String phase : new String[] {"open", "attempt", "login", "token_acquisition"}) {
            Attributes result = ConnectionAttributePolicy.span(input, "open".equals(phase), "attempt".equals(phase),
                    phase, null);
            assertEquals(1, result.size()); // only the constant database system name
            assertFalse(result.toString().contains("SECRET"));
        }
        input.put("mssql.timeout.phase", "SECRET");
        input.put("mssql.timeout.kind", "SECRET");
        input.put("mssql.timeout.value", Double.NaN);
        assertTrue(ConnectionAttributePolicy.diagnostic("mssql.driver.timeout", input).isEmpty());
    }
}
