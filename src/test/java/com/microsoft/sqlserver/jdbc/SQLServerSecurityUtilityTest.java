/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import reactor.core.Exceptions;

public class SQLServerSecurityUtilityTest {
    @Test
    public void testCredentialFailureIsPreserved() {
        IllegalStateException credentialFailure = new IllegalStateException("credential failure");
        SQLServerException exception = SQLServerSecurityUtility.mapTokenAcquisitionException(credentialFailure);

        assertEquals(SQLServerException.getErrString("R_ManagedIdentityTokenAcquisitionError"),
                exception.getMessage());
        assertSame(credentialFailure, exception.getCause());
    }

    @Test
    public void testTokenAcquisitionTimeoutIsMapped() {
        TimeoutException timeout = new TimeoutException("token acquisition timed out");
        SQLServerException exception = SQLServerSecurityUtility
                .mapTokenAcquisitionException(Exceptions.propagate(timeout));

        assertEquals(SQLServerException.getErrString("R_AADTokenAcquisitionTimeout"), exception.getMessage());
        assertSame(timeout, exception.getCause());
    }

    @Test
    public void testTokenAcquisitionInterruptionIsMapped() {
        InterruptedException interrupted = new InterruptedException("token acquisition interrupted");
        SQLServerException exception = SQLServerSecurityUtility
                .mapTokenAcquisitionException(Exceptions.propagate(interrupted));

        assertEquals(SQLServerException.getErrString("R_AADTokenAcquisitionInterrupted"), exception.getMessage());
        assertSame(interrupted, exception.getCause());
        assertFalse(Thread.currentThread().isInterrupted());
    }
}
