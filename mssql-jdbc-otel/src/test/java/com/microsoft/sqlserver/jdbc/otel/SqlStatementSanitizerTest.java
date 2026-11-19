/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class SqlStatementSanitizerTest {

    @Test
    void masksSingleQuotedUnicodeAndDoubleQuotedLiterals() {
        assertEquals("SELECT ?, ?", SqlStatementSanitizer.mask("SELECT N'SECRET', \"SECRET\""));
    }

    @Test
    void rejectsUnclosedDoubleQuotedLiteral() {
        assertNull(SqlStatementSanitizer.mask("SELECT \"SECRET"));
    }
}
