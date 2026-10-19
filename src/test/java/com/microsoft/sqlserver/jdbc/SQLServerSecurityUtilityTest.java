/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import reactor.core.publisher.Mono;


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
    public void workerInterruptionDoesNotInterruptCaller() {
        InterruptedException interrupted = new InterruptedException("token acquisition interrupted");
        RuntimeException credentialFailure = assertThrows(RuntimeException.class,
                () -> Mono.error(interrupted).blockOptional());

        try {
            SQLServerException exception = SQLServerSecurityUtility
                    .mapTokenAcquisitionException(credentialFailure);

            assertEquals(SQLServerException.getErrString("R_AADTokenAcquisitionInterrupted"),
                    exception.getMessage());
            assertSame(interrupted, exception.getCause());
            assertFalse(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void blockingWaitPreservesCallerInterruptFlag() {
        try {
            RuntimeException failure = assertThrows(RuntimeException.class, () -> Mono.never()
                    .doOnSubscribe(subscription -> Thread.currentThread().interrupt()).blockOptional());
            assertTrue(Thread.currentThread().isInterrupted());

            SQLServerException exception = SQLServerSecurityUtility.mapTokenAcquisitionException(failure);

            assertEquals(SQLServerException.getErrString("R_AADTokenAcquisitionInterrupted"), exception.getMessage());
            assertSame(failure.getCause(), exception.getCause());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}