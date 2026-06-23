/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;


/**
 * Regression tests for issue #2968: SQLServerError.getSQLServerMessage() returns 'this',
 * which exposes a self-referential JavaBeans property and causes infinite recursion in
 * bean-introspection serializers (Jackson, XMLEncoder, DataWeave, etc.).
 */
public class SQLServerErrorSerializationTest {

    /**
     * Verifies the fix: the SQLServerMessage descriptor is flagged transient via
     * {@code @java.beans.Transient}, so bean serializers skip it and don't recurse.
     */
    @Test
    public void getSQLServerMessagePropertyIsMarkedTransient() throws IntrospectionException {
        PropertyDescriptor sqlServerMessage = null;
        BeanInfo info = Introspector.getBeanInfo(SQLServerError.class);
        for (PropertyDescriptor pd : info.getPropertyDescriptors()) {
            if ("SQLServerMessage".equals(pd.getName())) {
                sqlServerMessage = pd;
                break;
            }
        }
        assertNotNull(sqlServerMessage,
                "Expected a 'SQLServerMessage' descriptor to exist - the getter is part of the "
                        + "ISQLServerMessage contract and is only expected to be flagged transient, not removed.");
        assertEquals(Boolean.TRUE, sqlServerMessage.getValue("transient"),
                "SQLServerError.getSQLServerMessage() must be flagged transient via @java.beans.Transient "
                        + "so bean serializers skip the self-referential property and do not recurse "
                        + "infinitely; see issue #2968.");
    }

    /**
     * Verifies the programmatic contract is preserved: getSQLServerMessage() still returns
     * the same instance. The fix only hides the getter from bean introspection.
     */
    @Test
    public void getSQLServerMessageStillReturnsSelf() {
        SQLServerError err = new SQLServerError();
        assertSame(err, err.getSQLServerMessage(),
                "getSQLServerMessage() must continue to return 'this' for direct callers");
    }

    /**
     * Verifies a SQLServerError serializes to JSON via Jackson without recursion,
     * and the output contains no 'SQLServerMessage' key.
     */
    @Test
    public void jacksonSerializesSQLServerErrorWithoutRecursion() throws Exception {
        SQLServerError err = new SQLServerError();
        err.setErrorNumber(2812);
        err.setErrorMessage("Could not find stored procedure 'XXX'.");

        String json = new ObjectMapper().writeValueAsString(err);

        assertFalse(json.toLowerCase().contains("sqlservermessage"),
                "Jackson output must not contain a 'SQLServerMessage' key for a SQLServerError - "
                        + "its presence means the self-referential getter is being walked; see issue #2968. "
                        + "Got: " + json);
        assertTrue(json.contains("2812"),
                "errorNumber must still be serialized after the fix. Got: " + json);
    }

    /**
     * Verifies serialization stays bounded even with Jackson's self-reference guard
     * disabled ({@code FAIL_ON_SELF_REFERENCES = false}).
     */
    @Test
    public void jacksonSerializationStaysBoundedWithSelfReferenceCheckDisabled() throws Exception {
        SQLServerError err = new SQLServerError();
        err.setErrorNumber(2812);
        err.setErrorMessage("Could not find stored procedure 'XXX'.");

        ObjectMapper mapper = new ObjectMapper().disable(SerializationFeature.FAIL_ON_SELF_REFERENCES);

        // If the self-referential cycle reappears, this throws (StackOverflowError /
        // nesting-limit error) and fails the test - which is the regression signal we want.
        String json = mapper.writeValueAsString(err);

        assertFalse(json.toLowerCase().contains("sqlservermessage"),
                "JSON must not contain a 'SQLServerMessage' key. Got: " + json);
    }
}
