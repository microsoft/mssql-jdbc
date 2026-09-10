/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;


@RunWith(JUnitPlatform.class)
public class SQLServerExceptionTest {

    @Test
    public void testMakeFromDriverErrorPreservesCause() {
        IOException ioException = new IOException("Connection reset");

        SQLServerException exception = assertThrows(SQLServerException.class, () -> SQLServerException
                .makeFromDriverError(null, null, ioException.getMessage(), null, true, ioException));

        assertSame(ioException, exception.getCause());
    }

    @Test
    public void testMakeFromDriverErrorWithoutCauseHasNoCause() {
        SQLServerException exception = assertThrows(SQLServerException.class,
                () -> SQLServerException.makeFromDriverError(null, null, "no cause", null, true));

        assertNull(exception.getCause());
    }
}
