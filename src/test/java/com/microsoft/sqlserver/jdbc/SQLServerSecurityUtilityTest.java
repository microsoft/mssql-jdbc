/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

public class SQLServerSecurityUtilityTest {
    @Test
    public void credentialFailureIsPreserved() {
        IllegalStateException credentialFailure = new IllegalStateException("credential failure");
        SQLServerException exception = SQLServerSecurityUtility.mapTokenAcquisitionException(credentialFailure);

        assertEquals(SQLServerException.getErrString("R_ManagedIdentityTokenAcquisitionError"),
                exception.getMessage());
        assertSame(credentialFailure, exception.getCause());
    }

    @Test
    public void tokenAcquisitionTimesOut() {
        TimeoutException timeout = new TimeoutException("token acquisition timed out");
        RuntimeException credentialFailure = new RuntimeException(timeout);
        SQLServerException exception = SQLServerSecurityUtility.mapTokenAcquisitionException(credentialFailure);

        assertEquals(SQLServerException.getErrString("R_AADTokenAcquisitionTimeout"), exception.getMessage());
        assertSame(timeout, exception.getCause());
    }

    @Test
    public void tokenAcquisitionRestoresInterruptFlag() {
        InterruptedException interrupted = new InterruptedException("token acquisition interrupted");
        RuntimeException credentialFailure = new RuntimeException(interrupted);

        try {
            SQLServerException exception = SQLServerSecurityUtility
                    .mapTokenAcquisitionException(credentialFailure);

            assertEquals(SQLServerException.getErrString("R_AADTokenAcquisitionInterrupted"),
                    exception.getMessage());
            assertSame(interrupted, exception.getCause());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}