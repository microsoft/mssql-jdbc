package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class TelemetryEventTest {

    @Test
    void testEventCapturesTelemetryAndAuthContext() {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Authorization", "Bearer token");
        headers.put("x-ms-client-request-id", "abc123");

        TelemetryEvent event = new TelemetryEvent(PerformanceActivity.LOGIN, 7, 0, 42L, null,
                "SELECT 1", StatementType.SELECT, headers, "Bearer", "jdbc.login", "00-111-222-00",
                "corr-123");

        assertEquals(PerformanceActivity.LOGIN, event.getActivity());
        assertEquals(42L, event.getDuration());
        assertEquals("SELECT 1", event.getUserSql());
        assertEquals(StatementType.SELECT, event.getStatementType());
        assertEquals("Bearer", event.getAuthScheme());
        assertEquals("Bearer token", event.getAuthHeaders().get("Authorization"));
        assertTrue(event.hasAuthContext());
        assertEquals("corr-123", event.getCorrelationId());
    }
}
