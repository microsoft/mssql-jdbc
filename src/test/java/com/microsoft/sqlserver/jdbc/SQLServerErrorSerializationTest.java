/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;

import org.junit.jupiter.api.Test;


/**
 * Regression tests for issue #2968: SQLServerError.getSQLServerMessage() returns 'this',
 * which exposes a self-referential JavaBeans property and causes infinite recursion in
 * bean-introspection serializers (Jackson, XMLEncoder, DataWeave, etc.).
 */
public class SQLServerErrorSerializationTest {

    /**
     * Verifies the fix: the self-referential getSQLServerMessage() getter must not be
     * exposed as a JavaBeans property. Achieved by annotating the getter with
     * @java.beans.Transient, which java.beans.Introspector honors.
     */
    @Test
    public void getSQLServerMessageIsNotABeanProperty() throws IntrospectionException {
        BeanInfo info = Introspector.getBeanInfo(SQLServerError.class);
        for (PropertyDescriptor pd : info.getPropertyDescriptors()) {
            assertFalse("SQLServerMessage".equals(pd.getName()),
                    "SQLServerError must not expose 'SQLServerMessage' as a JavaBeans property "
                            + "(self-referential getter causes infinite recursion in bean serializers; see issue #2968)");
        }
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
}
